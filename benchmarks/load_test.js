import http from 'k6/http';
import { check } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 100 },
    { duration: '60s', target: 500 },
    { duration: '30s', target: 0 },
  ],
};

export default function () {
  const key = `key_${Math.floor(Math.random() * 100000)}`;
  const setRes = http.put(`http://localhost:8080/api/v1/keys/${key}`, JSON.stringify({ value: 'testvalue', ttl: 60 }));
  check(setRes, { 'set status 200': (r) => r.status === 200 });
  const getRes = http.get(`http://localhost:8080/api/v1/keys/${key}`);
  check(getRes, { 'get status 200': (r) => r.status === 200 });
}
