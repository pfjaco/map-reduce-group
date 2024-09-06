"""This package is for code shared by the Manager and the Worker."""
from mapreduce.utils.network import tcp_send, tcp_receive, udp_receive
from mapreduce.utils.network import udp_send
from mapreduce.utils.convert_to_json import PathJSONEncoder
