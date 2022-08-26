import hmac
import time
from typing import Optional, Dict, Any, List
import json

from requests import Request, Session, Response

VERSION = 'v3/'

class YPClient:
    def __init__(
        self,
        base_url: str = "https://api.youngplatform.com/api/" + VERSION,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        subaccount_name: Optional[str] = None,
    ) -> None:
        self._session = Session()
        self._base_url = base_url
        self._api_key = api_key
        self._api_secret = api_secret
        self._subaccount_name = subaccount_name

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('GET', path, params=params)

    def _post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('POST', path, json=params)

    def _delete(self,
                path: str,
                params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('DELETE', path, json=params)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        request = Request(method, self._base_url + path, **kwargs)
        if self._api_key:
            self._sign_request(request)
        response = self._session.send(request.prepare())

        return self._process_response(response)

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time())
        prepared = request.prepare()
        signature_payload = {
            "recvWindow": 10,
            "timestamp": ts,
        }
        if prepared.body:
            signature_payload.update(json.loads(prepared.body))

        # Prepare alphabetically ordered body for hmac
        signature_payload_ordered_str = ''
        sorted_dict = {key: value for key, value in sorted(signature_payload.items())}
        for key in sorted_dict.keys():
            signature_payload_ordered_str += key + '=' + str(sorted_dict[key]) + '&'
        signature_payload_ordered_str = signature_payload_ordered_str[:-1]  # Remove final '&'

        signature = hmac.new(self._api_secret.encode(), signature_payload_ordered_str.encode(),
                             'sha512').hexdigest()
        request.headers['content-Type'] = 'application/json'
        request.headers['apiKey'] = self._api_key
        request.headers['hmac'] = signature
        request.data = json.dumps(signature_payload)


    @staticmethod
    def _process_response(response: Response) -> Any:
        try:
            data = response.json()
            return data
        except ValueError:
            response.raise_for_status()
            raise

    #
    # Public methods
    #

    def get_markets(self) -> List[dict]:
        return self._get('markets')

    def get_ticker(self, pair):
        return self._get(f'ticker?pair={pair}')

    def get_trades(self, pair):
        return self._get(f'trades?pair={pair}')

    def get_orderbook(self, pair, depth=10):
        return self._get(f'orderbook?pair={pair}&depth={str(depth)}')

    #
    # Authentication required methods
    #
    def authentication_required(fn):
        """Annotation for methods that require auth."""

        def wrapped(self, *args, **kwargs):
            if not self._api_key:
                raise TypeError("You must be authenticated to use this method")
            else:
                return fn(self, *args, **kwargs)

        return wrapped

    @authentication_required
    def get_wallet_balances(self) -> dict:
        return self._post('balances')

    @authentication_required
    def get_open_orders(self, pair) -> List[dict]:
        return self._post(f'openorders?pair={pair}')

    @authentication_required
    def place_market_order(self,
                    trade: str,  # First coin of the pair
                    market: str,  # Second coin fo the pair
                    side: str,  # BUY or SELL
                    volume: float,
                    ) -> dict:
        return self._post(
            'placeOrder', {
                "trade": trade,
                "market": market,
                "side": side,
                "type": "MARKET",
                "volume": volume,
            })

    @authentication_required
    def place_limit_order(self,
                           trade: str,  # First coin of the pair
                           market: str,  # Second coin fo the pair
                           side: str,  # BUY or SELL
                           volume: float,
                           rate: float  # Price
                           ) -> dict:
        return self._post(
            'placeOrder', {
                "trade": trade,
                "market": market,
                "side": side,
                "type": "LIMIT",
                "volume": volume,
                "rate": rate,
            })

    @authentication_required
    def get_order_status(self, existing_order_id: int) -> dict:
        return self._post(f'orderbyid?orderId={existing_order_id}')

    @authentication_required
    def cancel_order(self, side: str, orderId: int) -> dict:
        return self._post(f'cancelOrder', {"side": side, "orderId": orderId})

