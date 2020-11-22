import * as fs from 'fs';
import * as path from 'path';
import * as MongoClient from 'mongodb';

const configPath = path.resolve(__dirname, '../../config.json');
console.log('Config at', configPath);
const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
const uri = config.database_uri;

let _instance: MongoClient.Collection;

async function getCollection() {
  if (!_instance) {
    try {
      const client = await MongoClient.connect(uri, { useUnifiedTopology: true });
      _instance = client.db('lifeplusDb').collection('data');
    } catch (err) {
      throw err;
    }
  }
  return _instance;
}

export default getCollection; 