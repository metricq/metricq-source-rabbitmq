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
import logging

import click as click

import aiomonitor
import click_log
from metricq.logging import get_logger

from .source import RabbitMqSource

logger = get_logger()

click_log.basic_config(logger)
logger.setLevel("INFO")
logger.handlers[0].formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s"
)


@click.command()
@click.option("--server", default="amqp://localhost/")
@click.option("--token", default="source-rabbitmq")
@click.option("--monitor/--no-monitor", default=False)
@click.option("--log-to-journal/--no-log-to-journal", default=False)
@click_log.simple_verbosity_option(logger)
def source_cmd(server, token, monitor, log_to_journal):
    if log_to_journal:
        try:
            from systemd import journal

            logger.handlers[0] = journal.JournaldLogHandler()
        except ImportError:
            logger.error("Can't enable journal logger, systemd package not found!")

    src = RabbitMqSource(token=token, url=server)
    if monitor:
        with aiomonitor.start_monitor(src.event_loop, locals={"source": src}):
            src.run()
    else:
        src.run()
