from src.scenario_4_realtime_ingestion import start_streaming_pipeline

if __name__ == "__main__":
    query = start_streaming_pipeline(environment="local")
    query.awaitTermination()
