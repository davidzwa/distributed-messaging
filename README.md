# distributed-messaging
RabbitMQ based messaging simulator for Distributed Algorithms 

## Install and run project
_This is a temporary version, improvements expected before 1st of March, 2020._  

## Description
Simulate many independent algorithm nodes on a single or multiple pc, while being able to see and control the messaging traffic.

## Install and run
1) Install RabbitMQ-docker or RabbitMQ-server (as per https://www.rabbitmq.com/download.html)
2) Install python 3.7 (3.8 has some warnings in _asyncio_)
3) Install pipenv
4) Activate pipenv environment (f.e. [Example](https://thoughtbot.com/blog/how-to-manage-your-python-projects-with-pipenv))
5) Make sure: Install dependencies from Pipfile by running `pipenv install` (asyncio, aio_pika, etc.)
6) Run `docker-compose up -d` or start RabbitMQ-server (see step 1)
7) Run _main.py_ with `python -m main.py`

Optional:  
- open VSCode and run project from there
- open PyCharm and run project from there
- open any command prompt and run with step 7)
