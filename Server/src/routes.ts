import { Router, Request, Response } from 'express';
import getCollection from './connection';
import { Response as APIResponse, Request as APIRequest } from './classes';
import { StrToArr } from './types';

const routes = Router();

routes.get('/series', async (req: Request, res: Response) => {
  // if (!req.body.hasOwnProperty('query')) {
  //   return res.json(new APIResponse(false, undefined, undefined, undefined, 'Request must contain a query key'));
  // }
  const parsedReq = new APIRequest(req.body.query, req.body.len || -1, req.body.distinct);
  console.log('Parsed request is', parsedReq);
  const dbCollection = await getCollection();
  const cursor = dbCollection.find(parsedReq.query).limit(parsedReq.len);

  const distinctResults: StrToArr = {};
  if (parsedReq.distinct) {
    for (let field of parsedReq.distinct) {
      distinctResults[field] = await dbCollection.distinct(field, parsedReq.query);
    }
  }
  
  const preparedResponse = new APIResponse(true, await cursor.toArray(), await cursor.count(), distinctResults);
  if (parsedReq.len < 0) {
    preparedResponse.content = [];
  }
  return res.json(preparedResponse);
})

export default routes;