"""Tests for pymq.middleware module."""

import pytest
from pymq.middleware import chain_consumer, chain_producer


class TestChainConsumer:
    def test_empty(self):
        async def handler(d):
            pass

        result = chain_consumer(handler, [])
        assert result is handler

    def test_single(self):
        async def handler(d):
            pass

        def mw(next):
            async def wrapped(d):
                return await next(d)
            return wrapped

        result = chain_consumer(handler, [mw])
        assert result is not handler

    def test_order(self):
        order = []

        async def handler(d):
            order.append("handler")

        def mw1(next):
            async def wrapped(d):
                order.append("mw1-before")
                await next(d)
                order.append("mw1-after")
            return wrapped

        def mw2(next):
            async def wrapped(d):
                order.append("mw2-before")
                await next(d)
                order.append("mw2-after")
            return wrapped

        wrapped = chain_consumer(handler, [mw1, mw2])
        import asyncio
        asyncio.run(wrapped(None))
        # mw1 is outermost (first applied), mw2 is innermost
        assert order == ["mw1-before", "mw2-before", "handler", "mw2-after", "mw1-after"]
