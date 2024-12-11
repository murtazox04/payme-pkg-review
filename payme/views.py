import base64
import logging
import binascii
from decimal import Decimal

from django.conf import settings
from django.utils.module_loading import import_string

from rest_framework import views
from rest_framework.response import Response

from payme import exceptions
from payme.types import response
from payme.util import time_to_payme, time_to_service

logger = logging.getLogger(__name__)
TransactionModel = import_string(settings.PAYME_TRANSACTION_MODEL)


def handle_exceptions(func):
    """
    Decorator to handle exceptions and raise appropriate Payme exceptions.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TransactionModel.DoesNotExist as exc:
            logger.error(f"Transaction does not exist: {exc} {args} {kwargs}")
            raise exceptions.AccountDoesNotExist(str(exc)) from exc
        except exceptions.exception_whitelist as exc:
            raise exc
        except KeyError as exc:
            message = "Invalid parameters received."
            logger.error(f"{message}: {exc} {args} {kwargs}")
            raise exceptions.InternalServiceError(message) from exc
        except Exception as exc:
            logger.error(f"Unexpected error: {exc} {args} {kwargs}")
            raise exceptions.InternalServiceError(str(exc)) from exc
    return wrapper


class PaymeWebHookAPIView(views.APIView):
    """
    A webhook view for Payme using the custom Transaction model.
    """
    authentication_classes = ()

    def post(self, request, *args, **kwargs):
        """
        Handle the incoming webhook request.
        """
        self.check_authorize(request)

        payme_methods = {
            "GetStatement": self.get_statement,
            "CancelTransaction": self.cancel_transaction,
            "PerformTransaction": self.perform_transaction,
            "CreateTransaction": self.create_transaction,
            "CheckTransaction": self.check_transaction,
            "CheckPerformTransaction": self.check_perform_transaction,
        }

        try:
            method = request.data["method"]
            params = request.data["params"]
        except KeyError as exc:
            message = f"Error processing webhook: {exc}"
            raise exceptions.InternalServiceError(message) from exc

        if method in payme_methods:
            result = payme_methods[method](params)
            return Response(result)

        raise exceptions.MethodNotFound("Method not supported yet!")

    @staticmethod
    def check_authorize(request):
        """
        Verify the integrity of the request using the merchant key.
        """
        password = request.META.get('HTTP_AUTHORIZATION')
        if not password:
            raise exceptions.PermissionDenied("Missing authentication credentials")

        password = password.split()[-1]

        try:
            password = base64.b64decode(password).decode('utf-8')
        except (binascii.Error, UnicodeDecodeError) as exc:
            raise exceptions.PermissionDenied("Decoding error in authentication credentials") from exc

        try:
            payme_key = password.split(':')[-1]
        except IndexError as exc:
            message = "Invalid merchant key format in authentication credentials"
            raise exceptions.PermissionDenied(message) from exc

        if payme_key != settings.PAYME_KEY:
            raise exceptions.PermissionDenied("Invalid merchant key specified")

    @handle_exceptions
    def check_perform_transaction(self, params) -> response.CheckPerformTransaction:
        """
        Before creating a transaction, verify if you can perform it.
        For simplicity, just return allow=True if the transaction is valid.
        
        params['amount'] is in tiyin (1 sum = 100 tiyin).
        params['account'][settings.PAYME_ACCOUNT_FIELD] identifies the transaction.
        """
        # Example: use order_id from params to find the transaction.
        account_field = settings.PAYME_ACCOUNT_FIELD
        transaction_id = params['account'].get(account_field)
        if not transaction_id:
            raise exceptions.InvalidAccount("Missing account field in parameters.")

        # Get or validate transaction (for now, we just check existence)
        tran = TransactionModel.objects.get(id=transaction_id)
        
        # Validate amount matches tran.total_price * 100 if needed:
        received_amount = Decimal(params.get('amount', 0))
        expected_amount = tran.total_price * 100
        if received_amount != expected_amount:
            raise exceptions.IncorrectAmount(
                f"Invalid amount. Expected: {expected_amount}, received: {received_amount}"
            )

        result = response.CheckPerformTransaction(allow=True)
        return result.as_resp()

    @handle_exceptions
    def create_transaction(self, params) -> response.CreateTransaction:
        """
        Create a transaction in Payme's terms.
        Actually, transaction might already exist in your system.
        
        params['id'] is Payme's transaction_id.
        Use ext_id or create if needed.
        """
        payme_tr_id = params["id"]  # Payme's internal transaction ID
        account_field = settings.PAYME_ACCOUNT_FIELD
        transaction_id = params["account"].get(account_field)
        if not transaction_id:
            raise exceptions.InvalidAccount("Missing account field in parameters.")

        tran = TransactionModel.objects.get(id=transaction_id)
        # Validate amount
        received_amount = Decimal(params.get('amount', 0))
        expected_amount = tran.total_price * 100
        if received_amount != expected_amount:
            raise exceptions.IncorrectAmount(
                f"Invalid amount. Expected: {expected_amount}, received: {received_amount}"
            )

        # Set ext_id if needed
        if not tran.ext_id:
            tran.ext_id = payme_tr_id
            tran.save(update_fields=["ext_id"])

        # Payme considers INITIATING state as well
        # Your transaction status is "waiting" initially, map to INITIATING(1) or CREATED(0)
        # Just return current state mapped.
        result = response.CreateTransaction(
            transaction=tran.ext_id,
            state=tran.payme_state(),
            create_time=time_to_payme(tran.created_at),
        ).as_resp()

        self.handle_created_payment(params, result)
        return result

    @handle_exceptions
    def perform_transaction(self, params) -> response.PerformTransaction:
        """
        Mark the transaction as performed (successful).
        """
        payme_tr_id = params["id"]
        # Retrieve by ext_id
        tran = TransactionModel.objects.get(ext_id=payme_tr_id)

        if tran.is_performed():
            # Already performed
            result = response.PerformTransaction(
                transaction=tran.ext_id,
                state=tran.payme_state(),
                perform_time=time_to_payme(tran.confirmed_at),
            )
            return result.as_resp()

        # Perform the transaction
        success = tran.mark_as_performed()
        if not success:
            # If couldn't perform, maybe raise an error or handle accordingly
            raise exceptions.InternalServiceError("Cannot perform transaction")

        result = response.PerformTransaction(
            transaction=tran.ext_id,
            state=tran.payme_state(),
            perform_time=time_to_payme(tran.confirmed_at),
        ).as_resp()

        self.handle_successfully_payment(params, result)
        return result

    @handle_exceptions
    def check_transaction(self, params: dict):
        """
        Check the transaction status.
        """
        payme_tr_id = params["id"]
        tran = TransactionModel.objects.get(ext_id=payme_tr_id)

        # reason field can be deduced if you stored cancel_reason in tran.data
        reason = None
        if tran.data and "cancel_reason" in tran.data:
            reason = tran.data["cancel_reason"]

        result = response.CheckTransaction(
            transaction=tran.ext_id,
            state=tran.payme_state(),
            reason=reason,
            create_time=time_to_payme(tran.created_at),
            perform_time=time_to_payme(tran.confirmed_at),
            cancel_time=time_to_payme(tran.canceled_at),
        )
        return result.as_resp()

    @handle_exceptions
    def cancel_transaction(self, params) -> response.CancelTransaction:
        """
        Cancel the transaction.
        """
        payme_tr_id = params["id"]
        cancel_reason = params["reason"]
        tran = TransactionModel.objects.get(ext_id=payme_tr_id)

        if tran.is_cancelled():
            return self._cancel_response(tran)

        # If transaction performed, mark as canceled after success (-2)
        # If not performed, canceled during init (-1)
        payme_state = -1
        if tran.is_performed():
            payme_state = -2

        tran.mark_as_cancelled(cancel_reason=cancel_reason, payme_state=payme_state)
        result = self._cancel_response(tran)

        self.handle_cancelled_payment(params, result)
        return result

    @handle_exceptions
    def get_statement(self, params):
        """
        Retrieve a statement of transactions by date range.
        """
        from_time = time_to_service(params['from'])
        to_time = time_to_service(params['to'])

        transactions = TransactionModel.objects.filter(
            created_at__range=[from_time, to_time]
        ).order_by('-created_at')

        result = response.GetStatement(transactions=[])

        for t in transactions:
            reason = None
            if t.data and "cancel_reason" in t.data:
                reason = t.data["cancel_reason"]

            result.transactions.append({
                "transaction": t.ext_id or str(t.id),
                "amount": t.total_price * 100,  # in tiyin
                "account": {
                    settings.PAYME_ACCOUNT_FIELD: str(t.id)
                },
                "reason": reason,
                "state": t.payme_state(),
                "create_time": time_to_payme(t.created_at),
                "perform_time": time_to_payme(t.confirmed_at),
                "cancel_time": time_to_payme(t.canceled_at),
            })

        return result.as_resp()

    def _cancel_response(self, transaction):
        result = response.CancelTransaction(
            transaction=transaction.ext_id,
            state=transaction.payme_state(),
            cancel_time=time_to_payme(transaction.canceled_at),
        )
        return result.as_resp()

    def handle_created_payment(self, params, result, *args, **kwargs):
        print(f"Transaction created with params: {params}, result: {result}")

    def handle_successfully_payment(self, params, result, *args, **kwargs):
        print(f"Transaction successfully performed with params: {params}, result: {result}")

    def handle_cancelled_payment(self, params, result, *args, **kwargs):
        print(f"Transaction cancelled with params: {params}, result: {result}")
