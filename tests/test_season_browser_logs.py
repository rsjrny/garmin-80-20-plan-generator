from season_browser_logs import application_logs


def test_only_known_stdlib_socket_close_is_classified():
    close = '''Exception in callback _ProactorBasePipeTransport._call_connection_lost()
handle: <Handle _ProactorBasePipeTransport._call_connection_lost()>
Traceback (most recent call last):
  File "C:/Python/Lib/asyncio/events.py", line 89, in _run
    self._context.run(self._callback, *self._args)
  File "C:/Python/Lib/asyncio/proactor_events.py", line 165, in _call_connection_lost
    self._sock.shutdown(socket.SHUT_RDWR)
ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host
'''
    assert "Traceback" not in application_logs(close)
    application = close.replace("C:/Python/Lib/asyncio/proactor_events.py", "src/garmin_data_hub/services/season_generation.py")
    assert application_logs(application)==application
    assert "Traceback" in application_logs(close+"Traceback: real failure")
    assert application_logs(close.replace("10054", "10053"))==close.replace("10054", "10053")
