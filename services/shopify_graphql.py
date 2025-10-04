"""
Cliente Shopify GraphQL minimalista para operaciones comunes.
Inspirado en ../shopify-sync/src/shopify/api.py, adaptado a este proyecto.
"""
from __future__ import annotations

import time
import logging
from typing import Dict, Any, Optional, List
import requests
from config.settings import (
    SHOPIFY_SHOP_URL,
    SHOPIFY_ACCESS_TOKEN,
    SHOPIFY_API_VERSION,
    SHOPIFY_GQL_USE_SESSION,
    REQUEST_TIMEOUT,
)


logger = logging.getLogger(__name__)


class ShopifyGraphQL:
    def __init__(self, shop_url: Optional[str] = None, access_token: Optional[str] = None, api_version: Optional[str] = None):
        shop_url = shop_url or SHOPIFY_SHOP_URL
        access_token = access_token or SHOPIFY_ACCESS_TOKEN
        api_version = api_version or SHOPIFY_API_VERSION

        self.shop_url = shop_url.replace("https://", "").replace("http://", "").rstrip("/")
        self.access_token = access_token
        self.api_version = api_version
        self.endpoint = f"https://{self.shop_url}/admin/api/{self.api_version}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        }
        self.timeout = REQUEST_TIMEOUT or 30
        self.last_request_time = 0.0
        self.min_request_interval = 0.1
        self.current_retry = 0
        self.max_retries = 3
        self.retry_after = 0.0
        self.session = requests.Session() if SHOPIFY_GQL_USE_SESSION else None
        # Última info de throttling/coste reportada por GraphQL
        self.last_extensions: Dict[str, Any] | None = None

    def _handle_rate_limit(self) -> None:
        now = time.time()
        if self.retry_after > 0:
            time.sleep(self.retry_after)
            self.retry_after = 0
            return
        delta = now - self.last_request_time
        if delta < self.min_request_interval:
            time.sleep(self.min_request_interval - delta)
        self.last_request_time = time.time()

    def _request(self, query: str, variables: Dict[str, Any] | None = None) -> Dict[str, Any]:
        while True:
            self._handle_rate_limit()
            try:
                payload = {"query": query, "variables": variables or {}}
                if self.session is not None:
                    resp = self.session.post(self.endpoint, headers=self.headers, json=payload, timeout=self.timeout)
                else:
                    resp = requests.post(self.endpoint, headers=self.headers, json=payload, timeout=self.timeout)
                if resp.status_code == 429:
                    self.current_retry += 1
                    if self.current_retry > self.max_retries:
                        raise Exception("Rate limit: reintentos agotados")
                    self.retry_after = float(resp.headers.get("Retry-After", 5))
                    logger.warning(f"Rate limit excedido, esperando {self.retry_after}s")
                    continue
                resp.raise_for_status()
                data = resp.json()
                self.current_retry = 0
                if "errors" in data:
                    raise Exception(str(data["errors"]))
                # Guardar extensiones para control adaptativo (throttle/cost)
                try:
                    self.last_extensions = data.get("extensions")
                except Exception:
                    self.last_extensions = None
                return data.get("data", {})
            except requests.exceptions.RequestException as e:
                logger.error(f"GraphQL error: {e}")
                raise

    def get_product(self, product_id: str) -> Optional[Dict[str, Any]]:
        query = """
        query getProduct($id: ID!) {
          product(id: $id) {
            id
            title
            handle
            status
            variants(first: 50) {
              edges {
                node { id title sku price }
              }
            }
          }
        }
        """
        try:
            variables = {"id": f"gid://shopify/Product/{product_id}"}
            result = self._request(query, variables)
            return result.get("product")
        except Exception as e:
            logger.error(f"Error get_product {product_id}: {e}")
            return None

    def get_variant_info_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        query = """
        query($q: String!) {
          inventoryItems(first: 1, query: $q) {
            edges {
              node {
                id
                variant { id title product { id title } }
              }
            }
          }
        }
        """
        try:
            variables = {"q": f"sku:'{sku}'"}
            data = self._request(query, variables)
            edges = data.get("inventoryItems", {}).get("edges", [])
            if not edges:
                return None
            node = edges[0]["node"]
            variant = node.get("variant") or {}
            product = variant.get("product") or {}
            return {
                "inventory_item_id": node.get("id", "").split("/")[-1] if node.get("id") else None,
                "variant_id": variant.get("id", "").split("/")[-1] if variant.get("id") else None,
                "product_id": product.get("id", "").split("/")[-1] if product.get("id") else None,
                "product_title": product.get("title"),
            }
        except Exception as e:
            logger.error(f"Error get_variant_info_by_sku {sku}: {e}")
            return None

    def bulk_update_variant_price(self, product_id: str, variant_id: str, cost: float, margin: float = 2.5) -> bool:
        query = """
        mutation bulkUpdateVariants($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants { id price }
            userErrors { field message }
          }
        }
        """
        try:
            calculated_price = round(cost * margin, 2)
            variables = {
                "productId": f"gid://shopify/Product/{product_id}",
                "variants": [
                    {
                        "id": f"gid://shopify/ProductVariant/{variant_id}",
                        "price": str(calculated_price),
                        "inventoryItem": {"cost": cost},
                    }
                ],
            }
            result = self._request(query, variables)
            user_errors = result.get("productVariantsBulkUpdate", {}).get("userErrors", [])
            if user_errors:
                logger.error(f"bulk_update_variant_price errors: {user_errors}")
                return False
            return True
        except Exception as e:
            logger.error(f"Error bulk_update_variant_price: {e}")
            return False

    def product_variants_bulk_update(self, product_id: str, variants_input: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Actualiza múltiples variantes de un producto en una sola llamada.

        variants_input: lista de dicts con al menos {"id": gid_variant, "price": str, ...}
        Devuelve dict con claves: productVariants, userErrors
        """
        mutation = """
        mutation bulkUpdateVariants($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants { id price }
            userErrors { field message }
          }
        }
        """
        variables = {
            "productId": f"gid://shopify/Product/{product_id}",
            "variants": variants_input,
        }
        data = self._request(mutation, variables)
        return data.get("productVariantsBulkUpdate", {})

    def inventory_set_quantities(
        self,
        location_id: str,
        quantities: List[Dict[str, Any]],
        ignore_compare_quantity: bool = True,
        name: str = "available",
        reason: str = "correction",
        reference_document_uri: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Establece cantidades de inventario en bloque para múltiples inventory items.

        quantities: lista de dicts con keys: inventoryItemId (GID), locationId (GID), quantity, compareQuantity(optional)
        Devuelve dict con claves: inventoryAdjustmentGroup, userErrors
        """
        mutation = """
        mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
          inventorySetQuantities(input: $input) {
            inventoryAdjustmentGroup {
              reason
              referenceDocumentUri
              changes { name quantityAfterChange }
            }
            userErrors { code field message }
          }
        }
        """
        variables = {
            "input": {
                "ignoreCompareQuantity": bool(ignore_compare_quantity),
                "name": name,
                "reason": reason,
                "referenceDocumentUri": reference_document_uri or "system://queues/stock",
                "quantities": quantities,
            }
        }
        data = self._request(mutation, variables)
        return data.get("inventorySetQuantities", {})
