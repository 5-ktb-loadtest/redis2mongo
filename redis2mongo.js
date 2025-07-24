require('dotenv').config();
const Redis = require('ioredis');
const { MongoClient } = require('mongodb');

const redisCluster = new Redis.Cluster([
  { host: process.env.REDIS_HOST_1, port: Number(process.env.REDIS_PORT) },
  { host: process.env.REDIS_HOST_2, port: Number(process.env.REDIS_PORT) },
  { host: process.env.REDIS_HOST_3, port: Number(process.env.REDIS_PORT) },
  { host: process.env.REDIS_HOST_4, port: Number(process.env.REDIS_PORT) },
  { host: process.env.REDIS_HOST_5, port: Number(process.env.REDIS_PORT) },
  { host: process.env.REDIS_HOST_6, port: Number(process.env.REDIS_PORT) }
]);

const client = new MongoClient(process.env.MONGO_URI);
const dbName = 'backup';
const collectionName = 'redis_snapshot';

async function scanKeysAndBackup() {
  console.log('Connecting to Redis...');
  await client.connect();
  const db = client.db(dbName);
  const collection = db.collection(collectionName);

  console.log('Connected to Redis');

  const masters = redisCluster.nodes('master');
  let total = 0;

  for (const node of masters) {
    let cursor = '0';

    do {
      const [nextCursor, keys] = await node.scan(cursor, 'COUNT', 100);
      cursor = nextCursor;

      for (const key of keys) {
        try {
          const value = await node.get(key);
          if (value !== null) {
            await collection.updateOne(
              { key },
              { $set: { key, value, updatedAt: new Date() } },
              { upsert: true }
            );
            total++;
          }
        } catch (err) {
          console.error(`Error on key "${key}":`, err.message);
        }
      }
    } while (cursor !== '0');
  }

  await client.close();
  await redisCluster.quit();
  console.log(`[✓] Backup completed. Keys saved: ${total}`);
}

scanKeysAndBackup();
