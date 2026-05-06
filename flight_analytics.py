# Kütüphanelerim
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, DoubleType, TimestampType
)
import pymongo
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")
import pandas as pd
import os

# Ayarlar 

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV  = os.path.join(BASE_DIR, "flight_delays_synthetic.csv")
CSV_PATH     = os.environ.get("FLIGHT_CSV_PATH", DEFAULT_CSV)
MONGO_URI    = "mongodb://localhost:27017/"
MONGO_DB     = "flight_analytics"
MONGO_COL    = "flights"
STATS_COL    = "dashboard_stats" 
OUTPUT_DIR   = os.path.join(BASE_DIR, "charts")

os.makedirs(OUTPUT_DIR, exist_ok=True)

if not os.path.exists(CSV_PATH):
    raise FileNotFoundError(f"CSV dosyasi bulunamadi: {CSV_PATH}")


#  BÖLÜM 1 — SPARK OTURUMU

print("\n" + "="*60)
print("  BÖLÜM 1 — Spark Oturumu Başlatılıyor")
print("="*60)

spark = SparkSession.builder \
    .appName("FlightDelayAnalytics") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ SparkSession hazır.")


#  BÖLÜM 2 — VERİ YÜKLEME

schema = StructType([
    StructField("FlightID",           StringType(),    True),
    StructField("Airline",            StringType(),    True),
    StructField("FlightNumber",       StringType(),    True),
    StructField("Origin",             StringType(),    True),
    StructField("Destination",        StringType(),    True),
    StructField("ScheduledDeparture", TimestampType(), True),
    StructField("ActualDeparture",    TimestampType(), True),
    StructField("ScheduledArrival",   TimestampType(), True),
    StructField("ActualArrival",      TimestampType(), True),
    StructField("DelayMinutes",       IntegerType(),   True),
    StructField("DelayReason",        StringType(),    True),
    StructField("Cancelled",          StringType(),    True),
    StructField("Diverted",           StringType(),    True),
    StructField("AircraftType",       StringType(),    True),
    StructField("TailNumber",         StringType(),    True),
    StructField("Distance",           DoubleType(),    True),
])

df_raw = spark.read \
    .option("header", "true") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm") \
    .option("mode", "DROPMALFORMED") \
    .schema(schema) \
    .csv(CSV_PATH)

total_rows = df_raw.count()
print(f"✓ Ham veri yüklendi → {total_rows:,} satır")

#  BÖLÜM 3 — VERİ TEMİZLEME

df = df_raw.withColumn(
    "Cancelled", F.when(F.upper(F.col("Cancelled")) == "TRUE", True).otherwise(False)
).withColumn(
    "Diverted",  F.when(F.upper(F.col("Diverted"))  == "TRUE", True).otherwise(False)
)

df_active = df.filter(F.col("Cancelled") == False)
# Sadece gercekten geciken ucuslar analiz edilir; DelayMinutes = 0 olanlar ortalamayi dusurmez.
df_delayed = df_active.filter(
    F.col("DelayMinutes").isNotNull() & (F.col("DelayMinutes") > 0)
)

df_clean = df_delayed.withColumn(
    "DelayReason",
    F.when(F.col("DelayReason").isNull() | (F.trim(F.col("DelayReason")) == ""), "Unknown").otherwise(F.col("DelayReason"))
).withColumn(
    "DelayMinutes",
    F.when(F.col("DelayMinutes") < 0, 0).otherwise(F.col("DelayMinutes"))
)

#  BÖLÜM 4 — GECİKME ANALİZLERİ

# 4-A Airline Analizi
airline_delay = df_clean.groupBy("Airline").agg(
    F.round(F.avg("DelayMinutes"), 1).alias("AvgDelay"),
    F.count("*").alias("FlightCount")
).orderBy(F.desc("AvgDelay"))

# 4-B Origin Analizi
origin_delay = df_clean.groupBy("Origin").agg(
    F.round(F.avg("DelayMinutes"), 1).alias("AvgDelay"),
    F.count("*").alias("FlightCount")
).orderBy(F.desc("AvgDelay"))

#  BÖLÜM 5 — DELAY REASON ANALİZİ

# Spark bu listedeki isimleri gördüğünde onları "Other" yerine kendi adıyla bırakacak.
VALID_REASONS = [
    "Weather",
    "Airline",
    "Late Aircraft",
    "Air System",
    "Security",
    "Air Traffic Control",
    "Maintenance",
    "Crew Scheduling",
    "Baggage Handling",
    "Ground Operations",
    "Technical Inspection",
    "Passenger Issue",
    "Fueling",
]

COMPANY_REASONS = [
    "Airline",
    "Late Aircraft",
    "Maintenance",
    "Crew Scheduling",
    "Baggage Handling",
    "Ground Operations",
    "Technical Inspection",
    "Fueling",
]

EXTERNAL_REASONS = [
    "Weather",
    "Air System",
    "Security",
    "Air Traffic Control",
    "Passenger Issue",
]

# Eğer DelayReason sütunundaki değer VALID_REASONS listesinde varsa olduğu gibi bırak yoksa OTHER olarak işaretle

df_clean = df_clean.withColumn(
    "DelayCategory",
    F.when(F.col("DelayReason").isin(VALID_REASONS), F.col("DelayReason")).otherwise("Other")
).withColumn(
    "DelayResponsibility",
    F.when(F.col("DelayCategory").isin(COMPANY_REASONS), "Company")
    .when(F.col("DelayCategory").isin(EXTERNAL_REASONS), "External")
    .otherwise("Unknown")
)

# Gruplama yaparak her kategoriden kaç tane olduğunu sayıyo
reason_dist = df_clean.groupBy("DelayCategory").agg(
    F.count("*").alias("Count")
).orderBy(F.desc("Count"))

responsibility_dist = df_clean.groupBy("DelayResponsibility").agg(
    F.count("*").alias("Count"),
    F.round(F.avg("DelayMinutes"), 1).alias("AvgDelay")
).orderBy(F.desc("Count"))

airline_quality = df_clean.groupBy("Airline").agg(
    F.count("*").alias("DelayedFlightCount"),
    F.sum(F.when(F.col("DelayResponsibility") == "Company", 1).otherwise(0)).alias("CompanyDelayCount"),
    F.sum(F.when(F.col("DelayResponsibility") == "External", 1).otherwise(0)).alias("ExternalDelayCount"),
    F.round(F.avg("DelayMinutes"), 1).alias("AvgDelay"),
    F.round(
        F.avg(F.when(F.col("DelayResponsibility") == "Company", F.col("DelayMinutes"))),
        1
    ).alias("CompanyAvgDelay"),
).withColumn(
    "CompanyDelayRate",
    F.round((F.col("CompanyDelayCount") / F.col("DelayedFlightCount")) * 100, 1)
).withColumn(
    "ExternalDelayRate",
    F.round((F.col("ExternalDelayCount") / F.col("DelayedFlightCount")) * 100, 1)
).withColumn(
    "QualityScore",
    F.round(
        F.greatest(
            F.lit(0),
            F.lit(100)
            - (F.coalesce(F.col("CompanyAvgDelay"), F.lit(0)) * F.lit(0.45))
            - (F.col("CompanyDelayRate") * F.lit(0.35))
        ),
        1
    )
).orderBy(F.desc("QualityScore"))


reason_dist.show()


#  BÖLÜM 5.1 — MAINTENANCE GECİKMELERİ VE GÖRSELLEŞTİRME


# Sadece "Maintenance" olanları filtrele ve havayoluna göre grupla
maintenance_data = df_clean.filter(F.col("DelayReason") == "Maintenance") \
    .groupBy("Airline") \
    .count() \
    .orderBy(F.desc("count"))

# Spark verisini grafiğe dökmek için Pandas'a çevirdim
maintenance_pd = maintenance_data.toPandas()

# Matplotlib kullanarak grafik oluşturma
if not maintenance_pd.empty:
    plt.figure(figsize=(10, 6))
    plt.bar(maintenance_pd['Airline'], maintenance_pd['count'], color='orange')
    plt.title('Havayolu Şirketlerine Göre Bakım (Maintenance) Gecikmeleri')
    plt.xlabel('Havayolu Şirketi')
    plt.ylabel('Gecikme Sayısı')
    plt.xticks(rotation=45)
    

    chart_path = os.path.join(OUTPUT_DIR, "maintenance_analysis.png")
    plt.savefig(chart_path)
    plt.close()
    print(f"✓ Maintenance grafiği kaydedildi: {chart_path}")
else:
    print("! Maintenance verisi bulunamadığı için grafik oluşturulmadı.")

#  BÖLÜM 6 — MONGODB ENTEGRASYONU 

print("\n" + "="*60)
print("  BÖLÜM 6 — MongoDB Yazma İşlemleri")
print("="*60)

client = pymongo.MongoClient(MONGO_URI)
db = client[MONGO_DB]

# TEMİZLENMİŞ TÜM VERİYİ KAYDET 
CLEAN_DATA_COL = "cleaned_flights" 
print(f"  Temizlenmiş tüm veriler '{CLEAN_DATA_COL}' koleksiyonuna kaydediliyor...")
db[CLEAN_DATA_COL].drop() 

all_clean_data = [row.asDict() for row in df_clean.collect()]
if all_clean_data:
    db[CLEAN_DATA_COL].insert_many(all_clean_data)
    print(f"✓ {len(all_clean_data)} adet temiz kayıt kaydedildi.")

#  ÖZET TABLO 
print("  Django için özet istatistikler hazırlanıyor...")
db[STATS_COL].drop()

# Verileri MongoDB için listeye çeviriyor
airline_list = [row.asDict() for row in airline_delay.limit(10).collect()]
reason_list  = [row.asDict() for row in reason_dist.collect()]
origin_list  = [row.asDict() for row in origin_delay.collect()]
responsibility_list = [row.asDict() for row in responsibility_dist.collect()]
quality_list = [row.asDict() for row in airline_quality.collect()]

# Maintenance verisini hazırlıyo
maintenance_data = df_clean.filter(F.col("DelayReason") == "Maintenance") \
    .groupBy("Airline") \
    .count() \
    .orderBy(F.desc("count"))
maintenance_list = [row.asDict() for row in maintenance_data.collect()]


summary_doc = {
    "type": "dashboard_data",
    "total_processed": df_clean.count(),
    "airlines": airline_list,
    "reasons": reason_list,
    "responsibility": responsibility_list,
    "quality_ranking": quality_list,
    "airports": origin_list,
    "maintenance_stats": maintenance_list  
}

db[STATS_COL].insert_one(summary_doc)
print(f"✓ Özet istatistikler (Maintenance dahil) '{STATS_COL}' koleksiyonuna yazıldı.")
client.close()

#  BÖLÜM 7 — ÖZET RAPOR

print("\n" + "="*60)
print("  ANALİZ TAMAMLANDI")
print("="*60)
print(f"  Analiz Edilen Satır: {df_clean.count():,}")
print(f"  MongoDB Hedef DB   : {MONGO_DB}")
print("  Şimdi Django backend aşamasına geçebilirsin.")

spark.stop()
