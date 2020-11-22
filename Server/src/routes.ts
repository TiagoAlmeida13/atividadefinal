import dbCollection from './connection';
import express from 'express';

const routes = express.Router();

routes.get('/series', (req, res) => {
  return res.json({ 'message': 'Hi'});
})

export default routes;