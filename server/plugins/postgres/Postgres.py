from __future__ import annotations
from typing import Any
import os

from sqlalchemy import create_engine, MetaData, text
from server.plugins.PluginModels import 
from server.plugins.PluginResponse import PluginResponse
from server.plugins.PluginProtocol import Protocol

import logging
logger = logging.getLogger(__name__)


class Postgres(Protocol):
