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
from typing import Dict, Optional

import aiohttp
import metricq
from metricq import IntervalSource, Timestamp, get_logger
from yarl import URL

logger = get_logger()

__version__ = "0.2"


class RabbitMqSource(IntervalSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._vhosts = {}
        self._host: URL = None
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._prefix: str = ""
        self._last_queue_counts: Dict[str, float] = {}
        self._last_queue_timestamp: Dict[str, Timestamp] = {}
        self._last_exchange_counts: Dict[str, float] = {}
        self._last_exchange_timestamp: Dict[str, Timestamp] = {}

    @metricq.rpc_handler("config")
    async def _on_config(self, **config):
        self.period = config.get("interval", 1)
        self._prefix = config.get("prefix", "")
        self._host = URL(config.get("host"))
        self._username = config.get("username")
        self._password = config.get("password")

        metric_rate = 1.0 / self.period

        metrics = {}
        for vhost in config.get("vhosts", []):
            vhost_config = self._vhosts.get(vhost["name"], {})
            vhost_config.update(vhost)

            for exchange, exchange_config in vhost_config.get("exchanges", {}).items():
                metric_name_prefix = f"{self._prefix}.{vhost['name']}.exchange.{exchange.replace('.', '_').replace('-','_')}"
                for rate in exchange_config.get("rates", []):
                    metrics[f"{metric_name_prefix}.{rate}.rate"] = {
                        "unit": "msg/s",
                        "rate": metric_rate,
                    }

            for queue, queue_config in vhost_config.get("queues", {}).items():
                metric_name_prefix = f"{self._prefix}.{vhost['name']}.queue.{queue.replace('.', '_').replace('-','_')}"
                for rate in queue_config.get("rates", []):
                    metrics[f"{metric_name_prefix}.{rate}.rate"] = {
                        "unit": "msg/s",
                        "rate": metric_rate,
                    }
                for count in queue_config.get("counts", []):
                    metrics[f"{metric_name_prefix}.{count}.count"] = {
                        "unit": "msg",
                        "rate": metric_rate,
                    }

            self._vhosts[vhost["name"]] = vhost_config

        await self.declare_metrics(metrics)
        logger.info("declared {} metrics".format(len(metrics)))

    async def _update_exchanges(self, session: aiohttp.ClientSession, vhost: str):
        vhost_config = self._vhosts[vhost]
        try:
            async with session.get(
                (self._host / "api/exchanges").join(
                    URL(urllib.parse.quote_plus(vhost_config["vhost"]))
                )
            ) as resp:
                current_exchange_timestamp = Timestamp.now()
                try:
                    for exchange in await resp.json():
                        if exchange["name"] in vhost_config.get("exchanges", {}):
                            metric_name_prefix = f"{self._prefix}.{vhost}.exchange.{exchange['name'].replace('.', '_').replace('-', '_')}"
                            for rate in vhost_config["exchanges"][exchange["name"]].get(
                                "rates", []
                            ):
                                try:
                                    current_count = exchange["message_stats"][rate]
                                except KeyError:
                                    logger.debug(
                                        f"Message stat {rate} for exchange {exchange['name']} missing."
                                    )
                                    continue
                                metric_name = f"{metric_name_prefix}.{rate}.rate"

                                if (
                                    vhost in self._last_exchange_timestamp
                                    and metric_name in self._last_exchange_counts
                                ):
                                    diff = (
                                        current_exchange_timestamp
                                        - self._last_exchange_timestamp[vhost]
                                    )
                                    current_rate = (
                                        current_count
                                        - self._last_exchange_counts[metric_name]
                                    ) / diff.s
                                    logger.debug(
                                        f"{metric_name} rate is {current_rate}"
                                    )
                                    if current_rate >= 0:
                                        self[metric_name].append(
                                            current_exchange_timestamp, current_rate
                                        )
                                    else:
                                        logger.warn(
                                            f"Skipping negative rate {rate} for exchange {exchange['name']}."
                                        )
                                else:
                                    logger.debug(
                                        f"{metric_name} count is {current_count}"
                                    )
                                    logger.info(
                                        f"First request for {metric_name}. Only storing for internal state!"
                                    )
                                self._last_exchange_counts[metric_name] = current_count
                except aiohttp.ContentTypeError as exception:
                    logger.error(f"Can't decode json response! {exception}")
                self._last_exchange_timestamp[vhost] = current_exchange_timestamp
        except aiohttp.ClientConnectorError as exception:
            logger.error(f"Can't connect to rabbitmq! {exception}")

    async def _update_queues(self, session: aiohttp.ClientSession, vhost: str):
        vhost_config = self._vhosts[vhost]
        try:
            async with session.get(
                (self._host / "api/queues").join(
                    URL(urllib.parse.quote_plus(vhost_config["vhost"]))
                )
            ) as resp:
                current_queue_timestamp = Timestamp.now()
                try:
                    for queue in await resp.json():
                        if queue["name"] in vhost_config.get("queues", {}):
                            metric_name_prefix = f"{self._prefix}.{vhost}.queue.{queue['name'].replace('.', '_').replace('-', '_')}"
                            for rate in vhost_config["queues"][queue["name"]].get(
                                "rates", []
                            ):
                                try:
                                    current_count = queue["message_stats"][rate]
                                except KeyError:
                                    logger.debug(
                                        f"Message stat {rate} for queue {queue['name']} missing."
                                    )
                                    continue
                                metric_name = f"{metric_name_prefix}.{rate}.rate"

                                if (
                                    vhost in self._last_queue_timestamp
                                    and metric_name in self._last_queue_counts
                                ):
                                    diff = (
                                        current_queue_timestamp
                                        - self._last_queue_timestamp[vhost]
                                    )
                                    current_rate = (
                                        current_count
                                        - self._last_queue_counts[metric_name]
                                    ) / diff.s
                                    logger.debug(
                                        f"{metric_name} rate is {current_rate}"
                                    )
                                    if current_rate >= 0:
                                        self[metric_name].append(
                                            current_queue_timestamp, current_rate
                                        )
                                    else:
                                        logger.warn(
                                            f"Skipping negative rate {rate} for queue {queue['name']}."
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
                                try:
                                    current_count = queue[count]
                                except KeyError:
                                    logger.debug(
                                        f"Count {count} for queue {queue['name']} missing."
                                    )
                                    continue
                                metric_name = f"{metric_name_prefix}.{count}.count"
                                logger.debug(f"{metric_name} count is {current_count}")
                                self[metric_name].append(
                                    current_queue_timestamp, current_count
                                )
                except aiohttp.ContentTypeError as exception:
                    logger.error(f"Can't decode json response! {exception}")
                self._last_queue_timestamp[vhost] = current_queue_timestamp
        except aiohttp.ClientConnectorError as exception:
            logger.error(f"Can't connect to rabbitmq! {exception}")

    async def update(self):
        auth = None
        if self._username and self._password:
            auth = aiohttp.BasicAuth(login=self._username, password=self._password)
        async with aiohttp.ClientSession(auth=auth) as session:
            for vhost in self._vhosts:
                await self._update_exchanges(session, vhost)
                await self._update_queues(session, vhost)
            try:
                await self.flush()
            except Exception as e:
                logger.error(f"Exception in send: {e}")
