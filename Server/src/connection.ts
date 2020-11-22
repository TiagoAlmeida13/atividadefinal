import fs from 'fs';
import MongoClient from 'mongodb';

const config = JSON.parse(fs.readFileSync('../config.json', 'utf8'));
const uri = config.database_uri;

const dbCollection = new Promise<MongoClient.Collection>((resolve, reject) => {
  MongoClient.connect(uri, { useUnifiedTopology: true }, (err, client) => {
      if (err) reject(err); 
      const db = client.db('lifeplusDb');
      resolve(db.collection('data'));
  });
});

module.exports = (async () => await dbCollection)();