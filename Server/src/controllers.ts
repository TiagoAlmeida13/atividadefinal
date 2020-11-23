import { query } from 'express';
import { Collection } from 'mongodb';
import { Request, Response } from './classes';
import { objectHasAnyProperty} from './utils';

export class MongoDBController {

  collection: Collection;

  constructor(collection: Collection) {
    this.collection = collection;
  }

  async runQuery(request: Request) {
    let series, num, distinct;
    const queryExists = objectHasAnyProperty(request.query);
    const mongoLen = request.len.mongoLen;
    if (mongoLen === null) { // User wants only a count
      ({ num } = await this.getSeries(request.query, 0));
    } else if (queryExists && mongoLen >= 0) { // User wants results + count.
      ({ series, num } = await this.getSeries(request.query, mongoLen));
    }
    if (request.distinct && request.distinct.length > 0) {
      distinct = await this.getManyDistinct(request.distinct, request.query);
    }
    return new Response(true, series, num, distinct);
  }

  async countCollection() {
    return await this.collection.countDocuments();
  }

  async getSeries(query: Record<string, string>, limit: number) {
    console.log(`Making query limited to ${limit}:`, query);
    const cursor = this.collection.find(query).limit(limit);
    const series = await cursor.toArray();
    const num = await cursor.count();
    return { series, num };
  }

  getManyDistinct(fields: string[], query: Record<string, string>) {
    return fields.reduce(async (acc, field) => {
      const distincts = field === 'fields'
        ? await this.getSetFields(query)
	: await this.getOneDistinct(field, query);
      acc.then(obj => obj[field] = distincts);
      return acc;
    }, Promise.resolve(<Record<string, string[]>>{}))
  }

  async getOneDistinct(field: string, query: Record<string, string>): Promise<string[]> {
    console.log('Getting distinct:', field);
    return await this.collection.distinct(field, query);
  }

  async getSetFields(query: Record<string, string>): Promise<string[]> { // Improve this by getting result from MongoDB
    console.log('Getting set fields');
    const all = await this.collection.distinct('fields', query);
    const allKeys = all.reduce((acc, doc) => {
      Object.keys(doc).forEach(key => acc.add(key));
      return acc;
    }, new Set());
    return Array.from(allKeys);
  }

}
