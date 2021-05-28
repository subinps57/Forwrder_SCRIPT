import re
import pytz
from datetime import datetime
import os
import asyncio
from pyrogram import Client
from umongo import document
from pyrogram.errors import FloodWait
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow.exceptions import ValidationError
import os
DATABASE_URI = os.environ.get("DATABASE_URI", "mongodb+srv://subinps:subinps@cluster0.hjyoz.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
DATABASE_NAME = os.environ.get("DATABASE_NAME","Cluster0")
COLLECTION_NAME = os.environ.get('COLLECTION_NAME', 'Forward_files')

IST = pytz.timezone('Asia/Kolkata')
TO_CHANNEL=-1001337937992
SESSION="Forward"
OWNER=626664225
API_ID = 2736453
API_HASH = "bb7205e4aeec61c00ef1dcb94d3f91f1"
BOT_TOKEN=os.environ.get("BOT_TOKEN", "1889401159:AAELJrThf2gG_aS7qEq_frKC9gqhvGbT9DQ")


client = AsyncIOMotorClient(DATABASE_URI)
db = client[DATABASE_NAME]
instance = Instance(db)

@instance.register
class Data(Document):
    id = fields.StrField(attribute='_id')
    channel = fields.StrField()
    file_type = fields.StrField()
    file_size = fields.IntField()
    message_id = fields.IntField()
    use = fields.StrField()
    methord = fields.StrField()
    caption = fields.StrField()
    mime_type = fields.StrField()
    file_name = fields.StrField()

    class Meta:
        collection_name = COLLECTION_NAME
async def get_search_results():
    filter = {'use': "forward"}
    cursor = Data.find(filter)
    cursor.sort('$natural', -1)
    cursor.skip(0).limit(1)
    Messages = await cursor.to_list(length=1)
    return Messages

async def main():
    bot = Client(SESSION, API_ID, API_HASH, bot_token=BOT_TOKEN)
    await bot.start()
    mcount=0
    MessageCount=0
    

    try:
        m=await bot.send_message(chat_id=626664225, text="Forwarding Started")
        while await Data.count_documents() != 0:
            data = await get_search_results()
            for msg in data:
                channel=msg.channel
                file_id=msg.id
                message_id=msg.message_id
                methord = msg.methord
                caption = msg.caption
                file_type = msg.file_type
                file_size = msg.file_size
                file_name = msg.file_name
                mime_type = msg.mime_type
                chat_id=TO_CHANNEL
                if methord == "bot":
                    try:
                        await bot.copy_message(
                            chat_id=chat_id,
                            from_chat_id=channel,
                            parse_mode="md",
                            caption=f"<code>{file_name}</code>",
                            message_id=message_id
                            )
                        await asyncio.sleep(0.2)
                    except FloodWait as e:
                        datetime_ist = datetime.now(IST)
                        ISTIME = datetime_ist.strftime("%I:%M:%S %p - %d %B %Y")
                        await m.edit(text=f"Total Forwarded : <code>{MessageCount}</code>Sleeping for {e.x} Seconds\nLast Forwarded: <code>{file_name}</code>Last Forwarded at {ISTIME}")
                        await asyncio.sleep(e.x)
                        await bot.copy_message(
                            chat_id=chat_id,
                            from_chat_id=channel,
                            parse_mode="md",
                            caption=f"<code>{file_name}</code>",
                            message_id=message_id
                            )
                        await asyncio.sleep(0.2)
                    await Data.collection.delete_one({
                        'channel': channel,
                        'message_id': message_id,
                        'file_type': file_type,
                        'methord': "bot",
                        'use': "forward"
                        })
                    mcount += 1
                    MessageCount += 1
                    print(f"Forwarded: {file_name})
                    if mcount == 10:
                        try:
                            datetime_ist = datetime.now(IST)
                            ISTIME = datetime_ist.strftime("%I:%M:%S %p - %d %B %Y")
                            await m.edit(text=f"Total Forwarded : <code>{MessageCount}</code>\nLast Forwarded: <code>{file_name}</code>\nLast edited at {ISTIME}")
                            mcount -= 10
                        except FloodWait as e:
                            print(f"Floodwait {e.x}")
                            pass
                        except Exception as e:
                            await bot.send_message(chat_id=OWNER, text=f"LOG-Error: {e}")
                            print(e)
                            pass

        await bot.send_message(chat_id=626664225, text=f"Finished Forwarding\nTotal Indexed: {MessageCount}")
    finally:
        await bot.stop()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
