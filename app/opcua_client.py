from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)

try:
    from asyncua import Client, ua

    ASYNCUA_AVAILABLE = True
    _VARIANT_MAP: dict[str, Any] = {
        "i=1": ua.VariantType.Boolean,
        "i=2": ua.VariantType.SByte,
        "i=3": ua.VariantType.Byte,
        "i=4": ua.VariantType.Int16,
        "i=5": ua.VariantType.UInt16,
        "i=6": ua.VariantType.Int32,
        "i=7": ua.VariantType.UInt32,
        "i=8": ua.VariantType.Int64,
        "i=9": ua.VariantType.UInt64,
        "i=10": ua.VariantType.Float,
        "i=11": ua.VariantType.Double,
        "i=12": ua.VariantType.String,
        "i=13": ua.VariantType.DateTime,
        "i=15": ua.VariantType.ByteString,
    }
except ImportError:
    ASYNCUA_AVAILABLE = False
    _VARIANT_MAP = {}
    logger.warning("asyncua not installed – OPC UA features disabled")


# ── Lookup tables ────────────────────────────────────────────────────────────

DATA_TYPE_NAMES: dict[str, str] = {
    "i=1": "Boolean",
    "i=2": "SByte",
    "i=3": "Byte",
    "i=4": "Int16",
    "i=5": "UInt16",
    "i=6": "Int32",
    "i=7": "UInt32",
    "i=8": "Int64",
    "i=9": "UInt64",
    "i=10": "Float",
    "i=11": "Double",
    "i=12": "String",
    "i=13": "DateTime",
    "i=14": "Guid",
    "i=15": "ByteString",
    "i=16": "XmlElement",
    "i=17": "NodeId",
    "i=18": "ExpandedNodeId",
    "i=19": "StatusCode",
    "i=20": "QualifiedName",
    "i=21": "LocalizedText",
    "i=22": "ExtensionObject",
    "i=23": "DataValue",
    "i=24": "Variant",
    "i=25": "DiagnosticInfo",
}

ACCESS_LEVEL_BITS: dict[int, str] = {
    0: "CurrentRead",
    1: "CurrentWrite",
    2: "HistoryRead",
    3: "HistoryWrite",
    5: "SemanticChange",
    6: "StatusWrite",
    7: "TimestampWrite",
}

VALUE_RANK_NAMES: dict[int, str] = {
    -3: "ScalarOrOneDimension",
    -2: "Any",
    -1: "Scalar",
    0: "OneOrMoreDimensions",
    1: "OneDimension",
    2: "TwoDimensions",
    3: "ThreeDimensions",
}

NODE_CLASS_ICONS: dict[str, str] = {
    "Object": "folder",
    "Variable": "variable",
    "Method": "method",
    "ObjectType": "object-type",
    "VariableType": "variable-type",
    "DataType": "data-type",
    "ReferenceType": "reference-type",
    "View": "view",
}

# Node classes that may have hierarchical children
HAS_CHILDREN_CLASSES = frozenset(
    {"Object", "View", "ObjectType", "VariableType", "DataType", "ReferenceType"}
)

# Maximum number of historical values that may be fetched in a single call
_MAX_HISTORY_VALUES: int = 10_000


# ── Helpers ──────────────────────────────────────────────────────────────────


def _nid_str(node_id: Any) -> str:
    """Convert any NodeId/ExpandedNodeId form to a compact string."""
    raw: Any = node_id
    if hasattr(raw, "NodeId"):  # ExpandedNodeId
        raw = raw.NodeId
    s: str = raw.to_string() if hasattr(raw, "to_string") else str(raw)
    if s.startswith("ns=0;"):
        s = s[5:]
    return s


def _serialize(val: Any) -> Any:
    """Recursively convert OPC UA values to JSON-serialisable Python."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float, str)):
        return val
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, bytes):
        return val.hex()
    if isinstance(val, list):
        return [_serialize(v) for v in val]
    try:
        if hasattr(val, "to_string"):
            return val.to_string()
    except Exception:
        pass
    return str(val)


def _access_bits(level: Any) -> str:
    if level is None:
        return "N/A"
    try:
        level = int(level)
    except (TypeError, ValueError):
        return str(level)
    bits = [name for bit, name in ACCESS_LEVEL_BITS.items() if level & (1 << bit)]
    return ", ".join(bits) if bits else "None"


# ── Subscription handler ──────────────────────────────────────────────────────


class _SubHandler:
    def __init__(self, queue: asyncio.Queue, conn_id: int) -> None:
        self._queue = queue
        self._conn_id = conn_id

    def datachange_notification(self, node: Any, val: Any, data: Any) -> None:
        try:
            dv = data.monitored_item.Value
            item = {
                "type": "datachange",
                "conn_id": self._conn_id,
                "node_id": _nid_str(node.nodeid),
                "value": _serialize(val),
                "status_code": dv.StatusCode.name if dv.StatusCode else "Good",
                "source_timestamp": (
                    dv.SourceTimestamp.isoformat() if dv.SourceTimestamp else None
                ),
                "server_timestamp": (
                    dv.ServerTimestamp.isoformat() if dv.ServerTimestamp else None
                ),
            }
            self._queue.put_nowait(item)
        except Exception as exc:
            logger.debug("datachange_notification error: %s", exc)


# ── Manager ───────────────────────────────────────────────────────────────────


class OPCUAManager:
    """Singleton that tracks live OPC UA client connections."""

    def __init__(self) -> None:
        self._clients: dict[int, Any] = {}
        self._subscriptions: dict[int, Any] = {}
        self._sub_handlers: dict[int, _SubHandler] = {}
        self._sub_queues: dict[int, asyncio.Queue] = {}
        self._sub_handles: dict[str, Any] = {}

    # ── Connection lifecycle ───────────────────────────────────────────────

    def is_connected(self, conn_id: int) -> bool:
        return conn_id in self._clients

    def connected_ids(self) -> list[int]:
        return list(self._clients.keys())

    def get_client(self, conn_id: int) -> Any:
        if conn_id not in self._clients:
            raise ValueError(f"Connection {conn_id} is not active")
        return self._clients[conn_id]

    async def connect(
        self,
        conn_id: int,
        url: str,
        username: str = "",
        password: str = "",
        timeout: int = 10,
        security_mode: int = 1,
        security_policy: str = "None",
    ) -> dict:
        if not ASYNCUA_AVAILABLE:
            raise RuntimeError(
                "asyncua library is not installed. "
                "Run: pip install asyncua"
            )

        if conn_id in self._clients:
            await self.disconnect(conn_id)

        client = Client(url=url, timeout=timeout)
        if username:
            client.set_user(username)
            client.set_password(password or "")

        await client.connect()
        self._clients[conn_id] = client
        return await self._server_info(client)

    async def disconnect(self, conn_id: int) -> None:
        for key in [k for k in self._sub_handles if k.startswith(f"{conn_id}:")]:
            del self._sub_handles[key]
        for d in (self._sub_handlers, self._sub_queues):
            d.pop(conn_id, None)
        if conn_id in self._subscriptions:
            try:
                await self._subscriptions.pop(conn_id).delete()
            except Exception:
                pass
        if conn_id in self._clients:
            try:
                await self._clients.pop(conn_id).disconnect()
            except Exception:
                pass

    # ── Server info ───────────────────────────────────────────────────────

    async def get_server_info(self, conn_id: int) -> dict:
        return await self._server_info(self.get_client(conn_id))

    async def _server_info(self, client: Any) -> dict:
        info: dict[str, Any] = {"status": "connected"}
        try:
            endpoints = await client.get_endpoints()
            if endpoints:
                ep = endpoints[0]
                srv = ep.Server
                if srv:
                    info["server_name"] = (
                        srv.ApplicationName.Text if srv.ApplicationName else ""
                    )
                    info["server_uri"] = srv.ApplicationUri or ""
                    info["product_uri"] = srv.ProductUri or ""
                info["security_mode"] = (
                    ep.SecurityMode.name if ep.SecurityMode else "None"
                )
                info["security_policy"] = (
                    str(ep.SecurityPolicyUri).split("#")[-1]
                    if ep.SecurityPolicyUri
                    else "None"
                )
                info["endpoint_url"] = ep.EndpointUrl or ""
        except Exception:
            pass

        try:
            ns_array = await client.get_node("i=2255").read_value()
            info["namespaces"] = list(ns_array) if ns_array else []
        except Exception:
            pass

        try:
            status = await client.get_node("i=2256").read_value()
            if status:
                info["server_state"] = str(status.State)
                info["start_time"] = (
                    status.StartTime.isoformat()
                    if getattr(status, "StartTime", None)
                    else None
                )
                info["current_time"] = (
                    status.CurrentTime.isoformat()
                    if getattr(status, "CurrentTime", None)
                    else None
                )
                bi = getattr(status, "BuildInfo", None)
                if bi:
                    info["build_info"] = {
                        "product_name": str(getattr(bi, "ProductName", "")),
                        "product_uri": str(getattr(bi, "ProductUri", "")),
                        "manufacturer_name": str(
                            getattr(bi, "ManufacturerName", "")
                        ),
                        "software_version": str(
                            getattr(bi, "SoftwareVersion", "")
                        ),
                        "build_number": str(getattr(bi, "BuildNumber", "")),
                        "build_date": (
                            bi.BuildDate.isoformat()
                            if getattr(bi, "BuildDate", None)
                            else None
                        ),
                    }
        except Exception:
            pass

        return info

    # ── Browse ────────────────────────────────────────────────────────────

    async def browse(
        self, conn_id: int, node_id: Optional[str] = None
    ) -> list[dict]:
        client = self.get_client(conn_id)
        node = client.get_node(node_id) if node_id else client.get_node("i=84")

        refs = await node.get_references(
            refs=ua.ObjectIds.HierarchicalReferences,
            direction=ua.BrowseDirection.Forward,
            includesubtypes=True,
        )

        result = []
        for ref in refs:
            try:
                nid = _nid_str(ref.NodeId)
                nc = ref.NodeClass.name if ref.NodeClass else "Object"
                td = None
                if ref.TypeDefinition:
                    td_s = _nid_str(ref.TypeDefinition)
                    if td_s and td_s != "i=0":
                        td = td_s
                result.append(
                    {
                        "node_id": nid,
                        "browse_name": (
                            f"{ref.BrowseName.NamespaceIndex}:{ref.BrowseName.Name}"
                            if ref.BrowseName
                            else ""
                        ),
                        "display_name": (
                            ref.DisplayName.Text if ref.DisplayName else ""
                        ),
                        "node_class": nc,
                        "icon": NODE_CLASS_ICONS.get(nc, "node"),
                        "has_children": nc in HAS_CHILDREN_CLASSES,
                        "type_definition": td,
                    }
                )
            except Exception as exc:
                logger.debug("browse ref error: %s", exc)

        return result

    # ── Attributes ────────────────────────────────────────────────────────

    async def get_attributes(self, conn_id: int, node_id: str) -> dict:
        client = self.get_client(conn_id)
        node = client.get_node(node_id)
        attrs: dict[str, Any] = {}
        dv_info: dict | None = None
        refs_list: list[dict] = []

        # NodeClass first
        try:
            nc = await node.read_node_class()
            attrs["NodeClass"] = nc.name
        except Exception:
            nc = None

        # Common attributes
        _readers: list[tuple[str, Any]] = [
            ("NodeId", node.read_node_id),
            ("BrowseName", node.read_browse_name),
            ("DisplayName", node.read_display_name),
            ("Description", node.read_description),
            ("WriteMask", node.read_write_mask),
            ("UserWriteMask", node.read_user_write_mask),
        ]
        for name, fn in _readers:
            try:
                v = await fn()
                if name == "NodeId":
                    attrs[name] = _nid_str(v)
                elif name == "BrowseName":
                    attrs[name] = f"{v.NamespaceIndex}:{v.Name}"
                elif name in ("DisplayName", "Description"):
                    attrs[name] = v.Text if v else ""
                else:
                    attrs[name] = _serialize(v)
            except Exception:
                pass

        # Variable / VariableType
        if nc and nc.name in ("Variable", "VariableType"):
            for attr_name, fn in [
                ("DataType", node.read_data_type),
                ("ValueRank", node.read_value_rank),
                ("ArrayDimensions", node.read_array_dimensions),
                ("AccessLevel", node.read_access_level),
                ("UserAccessLevel", node.read_user_access_level),
                ("MinimumSamplingInterval", node.read_minimum_sampling_interval),
            ]:
                try:
                    v = await fn()
                    if attr_name == "DataType":
                        s = _nid_str(v)
                        attrs[attr_name] = {
                            "node_id": s,
                            "name": DATA_TYPE_NAMES.get(s, s),
                        }
                    elif attr_name in ("AccessLevel", "UserAccessLevel"):
                        attrs[attr_name] = {
                            "value": int(v),
                            "readable": _access_bits(v),
                        }
                    elif attr_name == "ValueRank":
                        attrs[attr_name] = {
                            "value": v,
                            "meaning": VALUE_RANK_NAMES.get(v, str(v)),
                        }
                    else:
                        attrs[attr_name] = _serialize(v)
                except Exception:
                    pass

            if nc.name == "Variable":
                try:
                    attrs["Historizing"] = await node.read_historizing()
                except Exception:
                    pass

            # Data value
            try:
                dv = await node.read_data_value()
                dv_info = {
                    "value": (
                        _serialize(dv.Value.Value) if dv.Value else None
                    ),
                    "status_code": dv.StatusCode.name if dv.StatusCode else "Good",
                    "status_code_value": (
                        dv.StatusCode.value if dv.StatusCode else 0
                    ),
                    "source_timestamp": (
                        dv.SourceTimestamp.isoformat()
                        if dv.SourceTimestamp
                        else None
                    ),
                    "server_timestamp": (
                        dv.ServerTimestamp.isoformat()
                        if dv.ServerTimestamp
                        else None
                    ),
                }
            except Exception as exc:
                logger.debug("read_data_value error: %s", exc)
                dv_info = {"error": type(exc).__name__}

        # Object
        if nc and nc.name == "Object":
            try:
                ev = await node.read_event_notifier()
                attrs["EventNotifier"] = {
                    "value": int(ev),
                    "subscribes_to_events": bool(int(ev) & 1),
                    "history_readable": bool(int(ev) & 4),
                    "history_writable": bool(int(ev) & 8),
                }
            except Exception:
                pass

        # Method
        if nc and nc.name == "Method":
            for attr_name, fn in [
                ("Executable", node.read_executable),
                ("UserExecutable", node.read_user_executable),
            ]:
                try:
                    attrs[attr_name] = await fn()
                except Exception:
                    pass

        # Type nodes
        if nc and nc.name in (
            "ObjectType",
            "VariableType",
            "DataType",
            "ReferenceType",
        ):
            try:
                attrs["IsAbstract"] = await node.read_is_abstract()
            except Exception:
                pass

        # ReferenceType extras
        if nc and nc.name == "ReferenceType":
            try:
                attrs["Symmetric"] = await node.read_symmetric()
            except Exception:
                pass
            try:
                inv = await node.read_inverse_name()
                attrs["InverseName"] = inv.Text if inv else ""
            except Exception:
                pass

        # References (capped at 100)
        try:
            for ref in (await node.get_references())[:100]:
                try:
                    rt = _nid_str(ref.ReferenceTypeId)
                    tgt = _nid_str(ref.NodeId)
                    refs_list.append(
                        {
                            "reference_type_id": rt,
                            "is_forward": ref.IsForward,
                            "node_id": tgt,
                            "browse_name": (
                                f"{ref.BrowseName.NamespaceIndex}:{ref.BrowseName.Name}"
                                if ref.BrowseName
                                else ""
                            ),
                            "display_name": (
                                ref.DisplayName.Text if ref.DisplayName else ""
                            ),
                            "node_class": (
                                ref.NodeClass.name if ref.NodeClass else ""
                            ),
                        }
                    )
                except Exception:
                    pass
        except Exception as exc:
            logger.debug("get_references error: %s", exc)

        return {
            "node_id": node_id,
            "attributes": attrs,
            "data_value": dv_info,
            "references": refs_list,
        }

    # ── Read ──────────────────────────────────────────────────────────────

    async def read_value(self, conn_id: int, node_id: str) -> dict:
        node = self.get_client(conn_id).get_node(node_id)
        dv = await node.read_data_value()
        return {
            "node_id": node_id,
            "value": _serialize(dv.Value.Value) if dv.Value else None,
            "status_code": dv.StatusCode.name if dv.StatusCode else "Good",
            "status_code_value": dv.StatusCode.value if dv.StatusCode else 0,
            "source_timestamp": (
                dv.SourceTimestamp.isoformat() if dv.SourceTimestamp else None
            ),
            "server_timestamp": (
                dv.ServerTimestamp.isoformat() if dv.ServerTimestamp else None
            ),
        }

    # ── Write ─────────────────────────────────────────────────────────────

    async def write_value(
        self,
        conn_id: int,
        node_id: str,
        value: Any,
        data_type_hint: Optional[str] = None,
    ) -> dict:
        node = self.get_client(conn_id).get_node(node_id)

        # Resolve current data type
        try:
            dt_nid = await node.read_data_type()
            dt_str: Optional[str] = _nid_str(dt_nid)
        except Exception:
            dt_str = data_type_hint

        converted = _coerce(value, dt_str)

        if dt_str and dt_str in _VARIANT_MAP:
            dv = ua.DataValue(ua.Variant(converted, _VARIANT_MAP[dt_str]))
            await node.write_attribute(ua.AttributeIds.Value, dv)
        else:
            await node.write_value(converted)

        return {"status": "ok", "message": "Value written successfully"}

    # ── Method call ───────────────────────────────────────────────────────

    async def call_method(
        self,
        conn_id: int,
        parent_id: str,
        method_id: str,
        args: list,
    ) -> Any:
        parent = self.get_client(conn_id).get_node(parent_id)
        result = await parent.call_method(method_id, *args)
        return _serialize(result)

    # ── Subscriptions ─────────────────────────────────────────────────────

    async def subscribe(
        self, conn_id: int, node_id: str, queue: asyncio.Queue
    ) -> None:
        client = self.get_client(conn_id)
        if conn_id not in self._subscriptions:
            handler = _SubHandler(queue, conn_id)
            sub = await client.create_subscription(500, handler)
            self._subscriptions[conn_id] = sub
            self._sub_handlers[conn_id] = handler
            self._sub_queues[conn_id] = queue
        else:
            if conn_id in self._sub_handlers:
                self._sub_handlers[conn_id]._queue = queue
            sub = self._subscriptions[conn_id]

        key = f"{conn_id}:{node_id}"
        if key not in self._sub_handles:
            handle = await sub.subscribe_data_change(client.get_node(node_id))
            self._sub_handles[key] = handle

    async def unsubscribe(self, conn_id: int, node_id: str) -> None:
        sub = self._subscriptions.get(conn_id)
        if not sub:
            return
        key = f"{conn_id}:{node_id}"
        handle = self._sub_handles.pop(key, None)
        if handle:
            try:
                await sub.unsubscribe(handle)
            except Exception:
                pass

    # ── Server diagnostics ────────────────────────────────────────────────

    async def get_server_diagnostics(self, conn_id: int) -> dict:
        """Read comprehensive OPC UA server diagnostics (sessions, subscriptions, capabilities)."""
        client = self.get_client(conn_id)
        diag: dict[str, Any] = {}

        # ServiceLevel i=2267 – 0..255; ≥200 = nominal, ≥100 = degraded
        try:
            sl = await client.get_node("i=2267").read_value()
            sl_int = int(sl)
            diag["service_level"] = sl_int
            diag["service_level_text"] = (
                "Gut" if sl_int >= 200
                else "Eingeschränkt" if sl_int >= 100
                else "Nicht verfügbar"
            )
        except Exception:
            pass

        # Auditing i=2994
        try:
            diag["auditing_enabled"] = bool(
                await client.get_node("i=2994").read_value()
            )
        except Exception:
            pass

        # ServerDiagnosticsSummary i=3706 (structured variable)
        _SUMMARY_FIELDS = [
            "ServerViewCount", "CurrentSessionCount", "CumulatedSessionCount",
            "SecurityRejectedSessionCount", "RejectedSessionCount",
            "SessionTimeoutCount", "SessionAbortCount",
            "CurrentSubscriptionCount", "CumulatedSubscriptionCount",
            "PublishingIntervalCount", "SecurityRejectedRequestsCount",
            "RejectedRequestsCount",
        ]
        try:
            summary = await client.get_node("i=3706").read_value()
            if summary:
                diag["diagnostics_summary"] = {
                    f: int(getattr(summary, f, 0) or 0)
                    for f in _SUMMARY_FIELDS
                    if getattr(summary, f, None) is not None
                }
        except Exception:
            pass

        # SubscriptionDiagnosticsArray i=2290
        _SUB_FIELDS = [
            "SubscriptionId", "Priority", "PublishingInterval",
            "MaxKeepAliveCount", "MaxLifetimeCount", "MaxNotificationsPerPublish",
            "PublishingEnabled", "ModifyCount", "EnableCount", "DisableCount",
            "RepublishRequestCount", "NotificationsCount",
            "DataChangeNotificationsCount", "EventNotificationsCount",
            "UnacknowledgedMessageCount", "CurrentMonitoredItemsCount",
        ]
        try:
            sub_arr = await client.get_node("i=2290").read_value()
            subs: list[dict] = []
            for sd in sub_arr or []:
                entry: dict[str, Any] = {}
                for f in _SUB_FIELDS:
                    v = getattr(sd, f, None)
                    if v is not None:
                        entry[f] = _serialize(v)
                if entry:
                    subs.append(entry)
            if subs:
                diag["subscription_diagnostics"] = subs
        except Exception:
            pass

        # SessionDiagnosticsArray i=3129
        _SESS_FIELDS = [
            "SessionId", "SessionName", "ServerUri", "EndpointUrl",
            "ActualSessionTimeout", "MaxResponseMessageSize",
            "ClientConnectionTime", "ClientLastContactTime",
            "CurrentSubscriptionsCount", "CurrentMonitoredItemsCount",
            "CurrentPublishRequestsInQueue", "TotalRequestCount",
        ]
        try:
            sess_arr = await client.get_node("i=3129").read_value()
            sessions: list[dict] = []
            for s in sess_arr or []:
                entry = {}
                for f in _SESS_FIELDS:
                    v = getattr(s, f, None)
                    if v is not None:
                        entry[f] = _serialize(v)
                cd = getattr(s, "ClientDescription", None)
                if cd:
                    entry["ClientDescription"] = {
                        "ApplicationName": _serialize(
                            getattr(cd, "ApplicationName", "")
                        ),
                        "ApplicationUri": str(getattr(cd, "ApplicationUri", "")),
                        "ApplicationType": str(getattr(cd, "ApplicationType", "")),
                    }
                if entry:
                    sessions.append(entry)
            if sessions:
                diag["session_diagnostics"] = sessions
        except Exception:
            pass

        # Operation limits (well-known capability nodes)
        _CAP_NODES: dict[str, str] = {
            "MaxBrowseContinuationPoints": "i=2740",
            "MaxQueryContinuationPoints": "i=2741",
            "MaxHistoryContinuationPoints": "i=2742",
            "MaxArrayLength": "i=11702",
            "MaxStringLength": "i=11703",
            "MaxByteStringLength": "i=12911",
            "MaxNodesPerRead": "i=11705",
            "MaxNodesPerWrite": "i=11707",
            "MaxNodesPerMethodCall": "i=11709",
            "MaxNodesPerBrowse": "i=11710",
            "MaxNodesPerTranslateBrowsePathsToNodeIds": "i=11712",
            "MaxMonitoredItemsPerCall": "i=11714",
            "MinSupportedSampleRate": "i=2278",
        }
        caps: dict[str, Any] = {}
        for name, nid in _CAP_NODES.items():
            try:
                v = await client.get_node(nid).read_value()
                caps[name] = _serialize(v)
            except Exception:
                pass
        if caps:
            diag["server_capabilities"] = caps

        # ServerProfileArray i=2272
        try:
            profiles = await client.get_node("i=2272").read_value()
            if profiles:
                diag["server_profiles"] = [str(p) for p in profiles]
        except Exception:
            pass

        # LocaleIdArray i=2273
        try:
            locales = await client.get_node("i=2273").read_value()
            if locales:
                diag["locale_ids"] = [str(lc) for lc in locales]
        except Exception:
            pass

        return diag

    # ── History ───────────────────────────────────────────────────────────

    async def read_history(
        self,
        conn_id: int,
        node_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        max_values: int = 200,
    ) -> list[dict]:
        """Read raw historical values for a Variable node."""
        from datetime import timezone

        node = self.get_client(conn_id).get_node(node_id)

        start_dt = (
            datetime.fromisoformat(start_time).replace(tzinfo=timezone.utc)
            if start_time
            else datetime(1970, 1, 1, tzinfo=timezone.utc)
        )
        end_dt = (
            datetime.fromisoformat(end_time).replace(tzinfo=timezone.utc)
            if end_time
            else datetime.now(timezone.utc)
        )

        history = await node.read_raw_history(
            starttime=start_dt,
            endtime=end_dt,
            numvalues=max(1, min(max_values, _MAX_HISTORY_VALUES)),
        )
        results = []
        for dv in history:
            results.append(
                {
                    "value": _serialize(dv.Value.Value) if dv.Value else None,
                    "status_code": dv.StatusCode.name if dv.StatusCode else "Good",
                    "status_code_value": (
                        dv.StatusCode.value if dv.StatusCode else 0
                    ),
                    "source_timestamp": (
                        dv.SourceTimestamp.isoformat()
                        if dv.SourceTimestamp
                        else None
                    ),
                    "server_timestamp": (
                        dv.ServerTimestamp.isoformat()
                        if dv.ServerTimestamp
                        else None
                    ),
                }
            )
        return results

    # ── Endpoint discovery ────────────────────────────────────────────────

    @staticmethod
    async def discover_endpoints(url: str, timeout: int = 10) -> list[dict]:
        """Discover available endpoints on an OPC UA server (no session required)."""
        client = Client(url=url, timeout=timeout)
        try:
            endpoints = await client.get_endpoints()
        except Exception:
            # Fallback: try connecting briefly
            try:
                await client.connect()
                endpoints = await client.get_endpoints()
                await client.disconnect()
            except Exception:
                raise
        result = []
        for ep in endpoints:
            result.append(
                {
                    "endpoint_url": ep.EndpointUrl or "",
                    "security_mode": (
                        ep.SecurityMode.name if ep.SecurityMode else "None"
                    ),
                    "security_policy": (
                        str(ep.SecurityPolicyUri).split("#")[-1]
                        if ep.SecurityPolicyUri
                        else "None"
                    ),
                    "transport_profile": (
                        str(ep.TransportProfileUri).split("/")[-1]
                        if ep.TransportProfileUri
                        else ""
                    ),
                    "server_name": (
                        ep.Server.ApplicationName.Text
                        if ep.Server and ep.Server.ApplicationName
                        else ""
                    ),
                    "server_uri": (
                        ep.Server.ApplicationUri if ep.Server else ""
                    ),
                    "security_level": (
                        int(ep.SecurityLevel)
                        if hasattr(ep, "SecurityLevel") and ep.SecurityLevel is not None
                        else 0
                    ),
                }
            )
        return result


# ── Type coercion helper ─────────────────────────────────────────────────────


_INT_TYPES: frozenset[str] = frozenset(
    {"i=2", "i=3", "i=4", "i=5", "i=6", "i=7", "i=8", "i=9"}
)
_FLOAT_TYPES: frozenset[str] = frozenset({"i=10", "i=11"})


def _coerce(value: Any, dt_str: Optional[str]) -> Any:

    if dt_str == "i=1":
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)
    if dt_str in _INT_TYPES:
        return int(value)
    if dt_str in _FLOAT_TYPES:
        return float(value)
    if dt_str == "i=12":
        return str(value)

    # Auto-detect
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        if value.lower() in ("true", "false"):
            return value.lower() == "true"
    return value


# ── Module-level singleton ────────────────────────────────────────────────────

opcua_manager = OPCUAManager()
