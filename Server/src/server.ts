import * as express from 'express';
import * as bodyParser from 'body-parser';
import * as cors from 'cors';
import routes from './routes';

const app = express();

app.use(bodyParser.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(cors());

app.use(routes);
app.listen(3333);
console.log('Listening at port 3333');