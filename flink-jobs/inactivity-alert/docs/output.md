Launch a `socketTextStream` on port `9999` in `taskmanager` and send some data to it using `netcat` or `telnet`. For example, you can run the following command in your taskmanager terminal:

```bash
nc -lk 9999
```

Then some sample input event in the format of `userId,message` can be sent to the socket, such as:

```
trung,say hello
trung,hahahaa
ron,greeting everyone
aiko,xin chao
```

If a particular user does not send any message for 10 seconds, an inactivity alert will be triggered for that user. 
The output will be printed in the console as follows:

## Output

```
output> 2026-04-03T16:44:06.708 - Received event for user: trung, message: say hello
output> 2026-04-03T16:44:14.325 - Received event for user: trung, message: hahahaa
output> 2026-04-03T16:44:24.325 - User: trung inactive for 10 seconds
output> 2026-04-03T16:45:23.324 - Received event for user: ron, message: greeting everyone
output> 2026-04-03T16:45:26.717 - Received event for user: aiko, message: xin chao
output> 2026-04-03T16:45:33.324 - User: ron inactive for 10 seconds
output> 2026-04-03T16:45:36.717 - User: aiko inactive for 10 seconds
```