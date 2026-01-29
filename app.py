from flask import Flask, jsonify
import psycopg2
from opentelemetry import trace
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter

resource = Resource(attributes={"service.name": "python-flask-demo"})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer_provider().get_tracer(__name__)

jaeger_exporter = JaegerExporter(
    agent_host_name="jaeger",
    agent_port=6831,
)
span_processor = BatchSpanProcessor(jaeger_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)
Psycopg2Instrumentor().instrument()

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "exampledb",
    "user": "exampleuser",
    "password": "examplepass"
}

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE
        )
    ''')
    c.execute('INSERT INTO users (name) VALUES (%s) ON CONFLICT DO NOTHING', ("Alice",))
    c.execute('INSERT INTO users (name) VALUES (%s) ON CONFLICT DO NOTHING', ("Bob",))
    conn.commit()
    c.close()
    conn.close()

@app.route('/')
def hello():
    return "Hello from OpenTelemetry + Jaeger tracing!"

@app.route('/users')
def get_users():
    with tracer.start_as_current_span("db-query"):
        conn = psycopg2.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute('SELECT id, name FROM users')
        rows = c.fetchall()
        c.close()
        conn.close()
    return jsonify(rows)

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=8000)
