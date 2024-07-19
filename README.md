# Preview movies stats using kafka and streamlit
- Add a Venv to run the Streamlit interface in /front/main.py
- You must have docker running
- Run Docker-compose.yaml (it will create the producer and the broker containers)
- Run main.scala in /kafka-stream/src/main/scala/org/esgi/project/main.scala

Preview results of kafka streaming in http://localhost:8080/{Routes in webserver}

Preview results of streamlit in http://localhost:8000
