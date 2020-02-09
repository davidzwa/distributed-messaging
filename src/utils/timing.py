import time


def getTime():
    return time.time()


def getElapsedTime(startTime, units='sec'):
    elapsedInSeconds = time.time() - startTime
    if units == 'sec':
        return elapsedInSeconds
    if units == 'min':
        return elapsedInSeconds/60
    if units == 'hour':
        return elapsedInSeconds/(60*60)
