import express from 'express';
import { Kafka } from 'kafkajs';
import morgan from 'morgan';

const app = express();
app.use(express.json());
app.use(morgan('dev'));

const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];
const kafka = new Kafka({ brokers: KAFKA_BROKERS });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'events-service-group' });

const TOPICS = ['movie-events', 'user-events', 'payment-events'];

async function startKafka() {
  const maxRetries = 10;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await producer.connect();
      await consumer.connect();
      for (const topic of TOPICS) {
        await consumer.subscribe({ topic, fromBeginning: true });
      }
      consumer.run({
        eachMessage: async ({ topic, message }) => {
          console.log(`Received from Kafka topic '${topic}': ${message.value.toString()}`);
        }
      });
      console.log(`Kafka connected on attempt ${attempt}`);
      return;
    } catch (err) {
      console.warn(`Kafka connection attempt ${attempt} failed:`, err.message);
      if (attempt === maxRetries) {
        console.error('Kafka connection failed after maximum retries. Exiting.');
        process.exit(1);
      }
      await new Promise(res => setTimeout(res, 2000));
    }
  }
}

startKafka().then(() => console.log('Kafka connected')).catch(err => {
  console.error('Kafka connection failed:', err);
  process.exit(1);
});

async function sendToKafka(topic, data) {
  await producer.send({
    topic,
    messages: [{ value: JSON.stringify(data) }]
  });
  console.log(`Sent to Kafka topic '${topic}':`, data);
}

app.post('/api/events/movie', async (req, res) => {
  await sendToKafka('movie-events', req.body);
  res.status(201).json({ status: 'success' });
});

app.post('/api/events/user', async (req, res) => {
  await sendToKafka('user-events', req.body);
  res.status(201).json({ status: 'success' });
});

app.post('/api/events/payment', async (req, res) => {
  await sendToKafka('payment-events', req.body);
  res.status(201).json({ status: 'success' });
});

app.get('/api/events/health', (req, res) => {
  res.json({ status: true });
});

const PORT = process.env.PORT || 8082;
app.listen(PORT, () => {
  console.log(`Events service listening on port ${PORT}`);
}); 