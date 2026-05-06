from django.shortcuts import render
from pymongo import MongoClient
import json

def dashboard_view(request):
    # MongoDbye bağlanmak için
    client = MongoClient("mongodb://localhost:27017/")
    db = client["flight_analytics"]
    collection = db["dashboard_stats"]

    # Sparkın hazırladığı özeti çekiyor
    data = collection.find_one({"type": "dashboard_data"})

    # Veriyi Chart.jsnin anlayacağı formata sok yani JSON string yap
    # Eğer veri henüz oluşmamışsa hata vermemesi için boş liste 
    context = {
        "airlines": json.dumps(data.get("airlines", []) if data else []),
        "reasons": json.dumps(data.get("reasons", []) if data else []),
        "airports": json.dumps(data.get("airports", []) if data else []),
        "maintenance": json.dumps(data.get("maintenance_stats", []) if data else []), 
        "total": data.get("total_processed", 0) if data else 0
    }

    return render(request, 'dashboard.html', context)