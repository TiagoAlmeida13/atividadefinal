import { Router, Request, Response } from 'express';
import { MongoDBController } from './controllers';
import getCollection from './connection';
import { Request as APIRequest, Response as APIResponse } from './classes';

const routes = Router();

routes.get('/series', async (req: Request, res: Response) => {
  let responseObj;
  try {
    const parsedReq = APIRequest.fromExpressRequest(req);
    console.log('Parsed request is', parsedReq);
    const dbCollection = await getCollection();
    const controller = new MongoDBController(dbCollection);
    responseObj = await controller.runQuery(parsedReq);
  } catch (err) {
    console.error(err);
    responseObj = new APIResponse(false, undefined, undefined, undefined, err.toString());
  }
  return res.json(responseObj);
})

export default routes;