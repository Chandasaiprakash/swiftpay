import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
    scenarios: {
        swiftpay_sustained_tps: {
            executor: 'constant-arrival-rate',
            rate: 250,              // Target: 250 transactions
            timeUnit: '1s',         // Per 1 second
            duration: '4s',      // 4000s runtime ensures exactly 1,000,000 iterations
            preAllocatedVUs: 150,   // Base pool of virtual threads ready to fire
            maxVUs: 600,            // Safety ceiling to absorb any transient downstream queues
        },
    },
    thresholds: {
        http_req_failed: ['rate<0.001'],   // 99.9% of requests must succeed (status 202)
        http_req_duration: ['p(95)<500'],  // 95% of requests must respond under 500ms
    },
};

export default function () {
    const url = 'http://localhost:8080/v1/payments';

    const payload = JSON.stringify({
        senderId: 1001,
        receiverId: 2005,
        amount: 100,
        currency: 'USD'
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': uuidv4(),
            'X-Correlation-ID': uuidv4()
        },
    };

    const response = http.post(url, payload, params);

    check(response, {
        'status is 202': (r) => r.status === 202
    });
}