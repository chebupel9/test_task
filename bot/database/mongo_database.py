import os

from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import timedelta

load_dotenv('.env')
address = os.getenv("MONGODB_ADDRESS")
username = os.getenv("DB_USER")
password = os.getenv("DB_PWD")
client = MongoClient(f"mongodb://{username}:{password}@{address}")
db = client["test_task"]
collection = db["sample_collection"]

def aggregate_data(dt_from, dt_upto, group_type):
    if group_type == 'month':
        month = [
            {
                "$match": {
                    "dt": {
                        "$gte": dt_from,
                        "$lte": dt_upto
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$dt"},
                        "month": {"$month": "$dt"}
                    },
                    "total_value": {"$sum": "$value"}
                }
            },
            {
                "$sort": {
                    "_id.year": 1,
                    "_id.month": 1
                }
            }
        ]
        monthData = list(collection.aggregate(month))

        existing_months = {entry['_id']['month'] for entry in monthData}

        dataset = [entry['total_value'] if entry['_id']['month'] in existing_months else 0 for entry in monthData]
        labels = [f"{entry['_id']['year']}-{entry['_id']['month']:02d}-01T00:00:00" for entry in monthData]

        return {"dataset": dataset, "labels": labels}

    elif group_type == "day":
        day = [
            {
                "$match": {
                    "dt": {
                        "$gte": dt_from,
                        "$lte": dt_upto
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$dt"},
                        "month": {"$month": "$dt"},
                        "day": {"$dayOfMonth": "$dt"},
                    },
                    "total_value": {"$sum": "$value"}
                }
            },
            {
                "$sort": {
                    "_id.year": 1,
                    "_id.month": 1,
                    "_id.day": 1
                }
            }
        ]

        dayData = list(collection.aggregate(day))
        all_days = [dt_from + timedelta(days=i) for i in range(int((dt_upto - dt_from).days) + 1)]

        aggregated_data = {tuple(entry['_id'].values()): entry['total_value'] for entry in dayData}

        dataset = [aggregated_data.get((day.year, day.month, day.day), 0) for day in all_days]
        labels = [day.isoformat() for day in all_days]

        return {"dataset": dataset, "labels": labels}

    elif group_type == "hour":
        hour = [
            {
                "$match": {
                    "dt": {
                        "$gte": dt_from,
                        "$lte": dt_upto
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$dt"},
                        "month": {"$month": "$dt"},
                        "day": {"$dayOfMonth": "$dt"},
                        "hour": {"$hour": "$dt"}
                    },
                    "total_value": {"$sum": "$value"}
                }
            },
            {
                "$sort": {
                    "_id.year": 1,
                    "_id.month": 1,
                    "_id.day": 1,
                    "_id.hour": 1
                }
            }
        ]

        all_hours = [dt_from + timedelta(hours=i) for i in range(int((dt_upto - dt_from).total_seconds() // 3600) + 1)]

        hourData = list(collection.aggregate(hour))

        dataset = []
        for i in range(len(all_hours)):
            try:
                dataset.append(hourData[i]['total_value'])
            except IndexError:
                dataset.append(0)

        labels = [hour.isoformat() for hour in all_hours]

        return {"dataset": dataset, "labels": labels}
