import asyncio
import json
from datetime import datetime, timezone, timedelta

from aiogram import types, Dispatcher

from bot.utils.project_utils import is_valid_json
from bot.database.mongo_database import aggregate_data


async def cmd_start(msg: types.Message) -> None:
    """Command start

    Args:
        msg (types.Message): msg object
    """
    
    await msg.answer(
        text='Отправьте данные в формате: {"dt_from": "2022-09-01T00:00:00",'
             ' "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}',
    )


async def text_message_handler(msg: types.Message) -> None:

    jsonData = is_valid_json(msg.text)

    if jsonData:
        if 'dt_from' in jsonData and 'dt_upto' in jsonData and 'group_type' in jsonData:
            dt_from = datetime.fromisoformat(jsonData['dt_from'])
            dt_upto = datetime.fromisoformat(jsonData['dt_upto'])
            group_type = jsonData['group_type']

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, lambda: aggregate_data(dt_from, dt_upto, group_type))

            await msg.answer(json.dumps(response, ensure_ascii=False))

        else:

            await msg.answer('Допустимо отправлять только следующие запросы: {"dt_from": "2022-09-01T00:00:00",'
                             ' "dt_upto": "2022-12-31T23:59:00", "group_type": "month"} {"dt_from": '
                             '"2022-10-01T00:00:00", "dt_upto": "2022-11-30T23:59:00", "group_type": '
                             '"day"} {"dt_from": "2022-02-01T00:00:00", "dt_upto": "2022-02-02T00:00:00", '
                             '"group_type": "hour"}')

    else:

        await msg.answer('Невалидные данные! Пример запроса: '
                         '{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}')


def register_user_handlers(dp: Dispatcher) -> None:
    """Register user handlers
    """

    dp.register_message_handler(cmd_start, commands=['start'])

    dp.register_message_handler(text_message_handler, content_types=types.ContentType.TEXT)


