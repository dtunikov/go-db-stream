import http from 'k6/http';

export let options = {
  scenarios: {
    fixed_rps: {
      executor: 'constant-arrival-rate',
      rate: 100,
      timeUnit: '1s', // per second
      duration: '1m',
      preAllocatedVUs: 50, // Allocate 50 virtual users
      maxVUs: 100, // Allow up to 100 VUs if needed
    },
  },
};

export default function () {
  const url = 'http://localhost:8082/create-user';
  const payload = JSON.stringify({
    name: 'John Doe',
  });
  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  http.post(url, payload, params);
}
