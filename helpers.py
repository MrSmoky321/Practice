from pathlib import Path
import socket
import inspect


TCP_RECEIVE_SIZE = 1452
_STRINGABLE = (Path, )

def to_dict(**kwargs):
    return {**kwargs}


def socket_is_connected(sock:socket.socket):
    # TODO: get some working wait to check if socket is still connected
    try:
        sock.send(bytearray())
        error = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    except Exception as e:
        error = 1
    return error == 0


def socket_receive(sock:socket.socket, size:int, scalar:dict=None):
    result = bytearray()
    while len(result) < size and socket_is_connected(sock):
        result += sock.recv(min(size - len(result),TCP_RECEIVE_SIZE))
    if len(result) != size:
        raise BrokenPipeError("Failed to receive all data!")
    if scalar is not None:
        result = int.from_bytes(result, **scalar)
    return result


class Dictable:

    def _conv_value(self, value, conv_to_str: bool):
        if value is None:
            return value
        if isinstance(value, (int, float, bool, str,)):
            return value
        if isinstance(value, (Path,)):
            return str(value)
        if isinstance(value, (list, tuple)):
            return [self._conv_value(v, conv_to_str) for v in value]
        if isinstance(value, dict):
            return {self._conv_value(k, conv_to_str): self._conv_value(v, conv_to_str) for k, v in value.items()}
        if hasattr(value, "to_dict") and callable(value.to_dict):
            return value.to_dict(conv_to_str)
        if conv_to_str:
            return str(value)
        return value

    def to_dict(self, conv_to_str: bool) -> dict:
        result = {}
        for k in dir(self):
            if k[:1] == "_":
                continue
            if callable(getattr(self, k)):
                continue
            result[k] = self._conv_value(getattr(self, k), conv_to_str)
        return result


def any_to_dict_list_scalar(inst, instance_map=None, lists_as_tuple=True):
    # NOTE: source from https://github.com/Godhart/vdf/blob/main/src/helpers.py
    if instance_map is None:
        instance_map = {}
    result = None
    if inst is None:
        pass
    elif isinstance(inst, (str, int, float, bool, )):
        result = inst
    elif inspect.isclass(inst):
        result = str(inst)
    elif isinstance(inst, _STRINGABLE):
        result = str(inst)
    elif isinstance(inst, dict) or (
            hasattr(inst, "items")  and callable(getattr(inst, "items"))
        and hasattr(inst, "keys")   and callable(getattr(inst, "keys"))
        and hasattr(inst, "values") and callable(getattr(inst, "values"))
    ):
        result = {}
        for k,v in inst.items():
            result[any_to_dict_list_scalar(k, instance_map, lists_as_tuple)] = \
                any_to_dict_list_scalar(v, instance_map, lists_as_tuple)
    elif hasattr(inst, "to_dict") and callable(getattr(inst, "to_dict")):
        if inst in instance_map:
            result = instance_map[inst]
        else:
            result = {}
            instance_map[inst] = result
            for k,v in inst.to_dict().items():
                result[k] = v
    elif isinstance(inst, (list, tuple)) or hasattr(inst, "__iter__"):
        result = []
        for v in inst:
            result.append(any_to_dict_list_scalar(v, instance_map, lists_as_tuple))
        if lists_as_tuple:
            result = tuple(result)
    elif " object at" not in str(inst):
        if inst in instance_map:
            result = instance_map[inst]
        else:
            instance_map[inst] = result
            result = str(inst)
    else:
        if inst in instance_map:
            result = instance_map[inst]
        else:
            result = {}
            instance_map[inst] = result
            for k in dir(inst):
                if k[:1] == "_":
                    continue
                v = getattr(inst, k)
                if callable(v) and inspect.isclass(v) is False:
                    continue
                result[k] = any_to_dict_list_scalar(v, instance_map, lists_as_tuple)
    if isinstance(result, dict):
        tmp = {}
        for k,v in sorted(result.items(), key=lambda x: str(x[0])):
            if not isinstance(k, (str, int, )):
                k = str(k)
            tmp[k] = v
        result = tmp
    return result


def to_dict(**kwargs):
    """
    Returns dict, constructed from kwargs
    """
    return {**kwargs}