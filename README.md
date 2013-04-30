zypre (fork)
============

A forked zypre I was playing with for another project of mine.
I did away with gevent as it's buggy and opted for IOLoop instead.
Also added more callbacks.

~trevorj 041013

zypre
=====

Python/gevent ZRE implementation

An initial stab at implemeting the ZRE protocol spoken by zyre in
python with gevent.

Currently very partial support UDP beacon sending, recving, socket
interconnection, heartbeating, and deadbeat detection.
