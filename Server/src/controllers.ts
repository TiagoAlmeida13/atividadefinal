import { Collection } from 'mongodb';
import { Request, Response } from './classes';

export class MongoDBController {

  collection: Collection;

  constructor(collection: Collection) {
    this.collection = collection;
  }

  async runQuery(request: Request) {
    let series, num, distinct;
    if (request.query && request.len > 0) { // For Mongo, a .limit() of 0 is the same as no limit.
      ({ series, num } = await this.getSeries(request.query, request.len));
    }
    if (request.distinct) {
      distinct = await this.getManyDistinct(request.distinct, request.query);
    }
    return new Response(true, series, num, distinct);
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
      acc[field] = await this.getOneDistinct(field, query);
      return acc;
    }, <Promise<Record<string, string[]>>>{})
  }

  async getOneDistinct(field: string, query: Record<string, string>): Promise<string[]> {
    console.log('Getting distinct:', field);
    return await this.collection.distinct(field, query);
  }

}