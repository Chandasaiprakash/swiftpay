import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {

    vus: 50,

    duration: '30s',

    thresholds: {
        http_req_failed: ['rate<0.01'],
        http_req_duration: ['p(95)<500']
    }
};

export default function () {

    const payload = JSON.stringify({
        senderId: 1001,
        receiverId: 2005,
        amount: 150.50,
        currency: 'USD'
    });

    const headers = {
        'Content-Type': 'application/json',
        'Idempotency-Key':
            `${__VU}-${__ITER}-${Date.now()}`
    };

    const response = http.post(
        'http://localhost:8080/v1/payments',
        payload,
        { headers }
    );

    check(response, {
        'status is 202':
            (r) => r.status === 202
    });

    sleep(1);
}