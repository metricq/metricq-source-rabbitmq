# metricq-source-rabbitmq
# Copyright (C) 2019 ZIH, Technische Universitaet Dresden, Federal Republic of Germany
#
# All rights reserved.
#
# This file is part of metricq-source-rabbitmq.
#
# metricq-source-rabbitmq is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# metricq-source-rabbitmq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with metricq-source-rabbitmq.  If not, see <http://www.gnu.org/licenses/>.
import urllib

import aiohttp
import metricq
from metricq import IntervalSource, Timestamp, get_logger
from yarl import URL

logger = get_logger()


class RabbitMqSource(IntervalSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._vhosts = {}
        self._host: URL = None
        self._username = None
        self._password = None
        self._prefix = ""
        self._last_queue_counts = {}
        self._last_queue_timestamp = None
        self._last_exchange_counts = {}
        self._last_exchange_timestamp = None

    @metricq.rpc_handler("config")
    async def _on_config(self, **config):
        self.period = config.get("interval", 1)
        self._prefix = config.get("prefix", "")
        self._host = URL(config.get("host"))
        self._username = config.get("username")
        self._password = config.get("password")

        rate = 1.0 / self.period

        metrics = {}
        for vhost in config.get("vhosts", []):
            vhost_config = self._vhosts.get(vhost["name"], {})
            vhost_config.update(vhost)

            for exchange, exchange_config in vhost_config.get("exchanges", {}).items():
                metric_name_prefix = f"{self._prefix}.{vhost['name']}.exchange.{exchange.replace('.', '_').replace('-','_')}"
                for rate in exchange_config.get("rates", []):
                    metrics[f"{metric_name_prefix}.rate.{rate}"] = {
                        "unit": "msg/s",
                        "rate": rate,
                    }

            for queue, queue_config in vhost_config.get("queues", {}).items():
                metric_name_prefix = f"{self._prefix}.{vhost['name']}.queue.{queue.replace('.', '_').replace('-','_')}"
                for rate in queue_config.get("rates", []):
                    metrics[f"{metric_name_prefix}.rate.{rate}"] = {
                        "unit": "msg/s",
                        "rate": rate,
                    }
                for count in queue_config.get("counts", []):
                    metrics[f"{metric_name_prefix}.count.{count}"] = {
                        "unit": "msg",
                        "rate": rate,
                    }

            self._vhosts[vhost["name"]] = vhost_config

        await self.declare_metrics(metrics)
        logger.info("declared {} metrics".format(len(metrics)))

    async def update(self):
        auth = None
        if self._username and self._password:
            auth = aiohttp.BasicAuth(login=self._username, password=self._password)
        async with aiohttp.ClientSession(auth=auth) as session:
            for vhost, vhost_config in self._vhosts.items():
                async with session.get(
                    self._host
                    / "api/exchanges"
                    / urllib.parse.quote_plus(vhost_config["vhost"])
                ) as resp:
                    current_exchange_timestamp = Timestamp.now()
                    for exchange in await resp.json():
                        if exchange["name"] in vhost_config.get("exchanges", {}):
                            metric_name_prefix = f"{self._prefix}.{vhost}.exchange.{exchange['name'].replace('.', '_').replace('-', '_')}"
                            for rate in vhost_config["exchanges"][exchange["name"]].get(
                                "rates", []
                            ):
                                current_count = exchange["message_stats"][rate]
                                metric_name = f"{metric_name_prefix}.rate.{rate}"

                                if (
                                    self._last_exchange_timestamp is not None
                                    and metric_name in self._last_exchange_counts
                                ):
                                    diff = (
                                        current_exchange_timestamp
                                        - self._last_exchange_timestamp
                                    )
                                    current_rate = (
                                        current_count
                                        - self._last_exchange_counts[metric_name]
                                    ) / diff.s
                                    logger.debug(
                                        f"{metric_name} rate is {current_rate}"
                                    )
                                    self[metric_name].append(
                                        current_exchange_timestamp, current_rate
                                    )
                                else:
                                    logger.debug(
                                        f"{metric_name} count is {current_count}"
                                    )
                                    logger.info(
                                        f"First request for {metric_name}. Only storing for internal state!"
                                    )
                                self._last_exchange_counts[metric_name] = current_count
                    self._last_exchange_timestamp = current_exchange_timestamp

                async with session.get(
                    self._host
                    / "api/queues"
                    / urllib.parse.quote_plus(vhost_config["vhost"])
                ) as resp:
                    current_queue_timestamp = Timestamp.now()
                    for queue in await resp.json():
                        if queue["name"] in vhost_config.get("queues", {}):
                            metric_name_prefix = f"{self._prefix}.{vhost}.queue.{queue['name'].replace('.', '_').replace('-','_')}"
                            for rate in vhost_config["queues"][queue["name"]].get(
                                "rates", []
                            ):
                                current_count = queue["message_stats"][rate]
                                metric_name = f"{metric_name_prefix}.rate.{rate}"

                                if (
                                    self._last_queue_timestamp is not None
                                    and metric_name in self._last_queue_counts
                                ):
                                    diff = (
                                        current_queue_timestamp
                                        - self._last_queue_timestamp
                                    )
                                    current_rate = (
                                        current_count
                                        - self._last_queue_counts[metric_name]
                                    ) / diff.s
                                    logger.debug(
                                        f"{metric_name} rate is {current_rate}"
                                    )
                                    self[metric_name].append(
                                        current_queue_timestamp, current_rate
                                    )
                                else:
                                    logger.debug(
                                        f"{metric_name} count is {current_count}"
                                    )
                                    logger.info(
                                        f"First request for {metric_name}. Only storing for internal state!"
                                    )
                                self._last_queue_counts[metric_name] = current_count
                            for count in vhost_config["queues"][queue["name"]].get(
                                "counts", []
                            ):
                                current_count = queue[count]
                                metric_name = f"{metric_name_prefix}.count.{count}"
                                logger.debug(f"{metric_name} count is {current_count}")
                                self[metric_name].append(
                                    current_queue_timestamp, current_count
                                )
                    self._last_queue_timestamp = current_queue_timestamp
            try:
                await self.flush()
                pass
            except Exception as e:
                logger.error(f"Exception in send: {e}")
