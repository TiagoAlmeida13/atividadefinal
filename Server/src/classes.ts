import * as assert from 'assert';
import { Request as ExpressRequest } from 'express';

export class Response {

  success: Boolean;
  content?: Array<any>;
  len?: number;
  distinct?: Record<string, string[]>;
  error?: string;

  constructor(success: Boolean, content?: Array<any>, len?: number, distinct?: Record<string, string[]>, error?: string) {
    this.success = success;
    // If there's content, there must be a len
    if (content) {
      assert.notStrictEqual(len, undefined);
      this.content = content;
      this.len = len;
    }
    if (distinct) this.distinct = Response.parseDistinct(distinct);
    if (error) this.error = error;
  }

  static parseDistinct(distinct: Record<string, string[]>) {
    const toRemove = 'fields.'
    for (const k in distinct) {
      if (distinct.hasOwnProperty(k) && k.startsWith(toRemove)) {
        const shortenedKey = k.replace(toRemove, '');
        distinct[shortenedKey] = distinct[k];
        delete distinct[k];
      }
    }
    return distinct;
  }

}

export class Request {

  query: Record<string, string>;
  len: RequestLen;
  distinct?: Array<string>;

  constructor(query: string, len: number, distinct?: Array<string>) {
    this.query = Request.parseQueryString(query);
    this.len = new RequestLen(len);
    if (typeof distinct !== 'undefined' && !Array.isArray(distinct)) {
      throw 'If provided, distinct must be an array of strings';
    }
    this.distinct = distinct?.map(field => field === 'fields' ? field : `fields.${field}`);
  }

  static fromExpressRequest(expressRequest: ExpressRequest) {
    let { query, len, distinct } = expressRequest.query;
    if (typeof query === 'object' || typeof len === 'object') {
      throw 'Accepts only a single query';
    }
    if (typeof distinct === 'string') {
      distinct = [distinct];
    } else if (!(typeof distinct === 'undefined')) {
      throw 'Distinct must be string or array of string';
    }
    return new Request(query || '', Number(len), distinct);
  }

  static parseQueryString(query: string) {
    let parsedQuery = <Record<string, string>>{};

    if (query) {
      const spacesNotInsideQuotes = /\s(?=(?:[^'"`]*(['"`])[^'"`]*\1)*[^'"`]*$)/;
      const queryAsNestedArray = query
        .split(spacesNotInsideQuotes)
	.filter(el => typeof el !== 'undefined')
	.map(part => part.replace(/"/g, '').split('='));
      for (let [k, v] of queryAsNestedArray) {
        if (k === 'uid') {
          parsedQuery._id = v;
        } else {
          parsedQuery[`fields.${k}`] = v;
        }
      }
    }

    return parsedQuery;
  }

}

export class RequestLen {

  requestLen: number

  constructor(len: number) {
    if (len < -1) {
      throw 'len must be -1 or higher';
    }
    this.requestLen = len;
  }

  toMongo() {
    if (this.requestLen === -1) {
      return 0; // For Mongo, a limit of 0 == no limit.
    } else if (this.requestLen === 0) {
      return null; // Mongo has no thing as 'I want to query, but get no documents'
    } else { // > 0
      return this.requestLen;
    }
  }

  toString() {
    return `RequestLen(${this.requestLen})`;
  }
}