# QUEUE
In order to  reduce the degree of coupling within the code, our team design a queue based on characteristic of golang.
The queue contains message, messagepool, topic, client, and queue, 
every module of code has client， they communicate by client'queue.