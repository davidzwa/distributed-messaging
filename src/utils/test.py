import asyncio

async def test_async():
    print('Hello ...')
    await asyncio.sleep(1)
    print('... World!')
