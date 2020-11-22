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
  len: number;
  distinct?: Array<string>;

  constructor(query: string, len: number, distinct?: Array<string>) {
    this.query = Request.parseQueryString(query);
    this.len = len;
    if (typeof distinct !== 'undefined' && !Array.isArray(distinct)) {
      throw 'If provided, distinct must be an array of strings';
    }
    this.distinct = distinct?.map(field => field === 'fields' ? field : `fields.${field}`);
  }

  static fromExpressRequest(expressRequest: ExpressRequest) {
    const reqBody = expressRequest.body;
    return new Request(reqBody.query, reqBody.len, reqBody.distinct);
  }

  static parseQueryString(query: string) {
    let parsedQuery = <Record<string, string>>{};

    if (query) {
      const queryAsNestedArray = query.split(' ').map(part => part.split('='));
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