import asyncio
import aiohttp
from tqdm import tqdm
import json
from datetime import datetime
from dotenv import load_dotenv
import os

class ShopifyBulkDeleter:
    def __init__(self, shop_url=None, access_token=None, simulation_mode=False):
        load_dotenv()
        self.shop_url = shop_url or os.getenv('SHOPIFY_SHOP_URL')
        self.access_token = access_token or os.getenv('SHOPIFY_ACCESS_TOKEN')
        self.api_version = '2024-01'
        self.base_url = f"https://{self.shop_url}/admin/api/{self.api_version}"
        self.headers = {
            'X-Shopify-Access-Token': self.access_token,
            'Content-Type': 'application/json'
        }
        self.simulation_mode = simulation_mode
        self.batch_size = 20  # Aumentado a 20 productos por lote
        self.max_concurrent = 5  # Máximo de operaciones concurrentes
        self.processed_products = []
        self.errors = []

    async def get_total_products(self, session):
        url = f"{self.base_url}/products/count.json"
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            return data['count']

    async def get_product_batch(self, session, limit=20):
        url = f"{self.base_url}/products.json"
        params = {
            'limit': limit,
            'fields': 'id,title'
        }
        
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            return data['products']

    async def delete_product(self, session, product):
        """Borra un producto individual"""
        if self.simulation_mode:
            await asyncio.sleep(0.1)
            return True, None

        try:
            url = f"{self.base_url}/products/{product['id']}.json"
            async with session.delete(url) as response:
                if response.status == 200:
                    return True, None
                # Si recibimos 429, propagamos el error para manejarlo arriba
                if response.status == 429:
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=429
                    )
                error_text = await response.text()
                return False, f"Error {response.status}: {error_text}"
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                raise
            return False, str(e)
        except Exception as e:
            return False, str(e)

    async def delete_product_batch(self, session, products):
        """Borra un lote de productos con concurrencia limitada"""
        tasks = []
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def delete_with_semaphore(product):
            async with semaphore:
                return await self.delete_product(session, product)

        tasks = [delete_with_semaphore(product) for product in products]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = []
        failed = []
        
        for product, result in zip(products, results):
            if isinstance(result, Exception):
                if isinstance(result, aiohttp.ClientResponseError) and result.status == 429:
                    raise result
                failed.append((product, str(result)))
            else:
                success, error = result
                if success:
                    successful.append(product)
                else:
                    failed.append((product, error))
        
        return successful, failed

    def update_progress(self, pbar, status_bar, batch_size, current_batch, total_batches):
        pbar.update(batch_size)
        status_msg = (f"Lote {current_batch}/{total_batches} | "
                     f"Productos procesados: {len(self.processed_products)}")
        if self.errors:
            status_msg += f" | Errores: {len(self.errors)}"
        status_bar.set_description(status_msg)

    async def delete_all_products(self):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            print(f"\nModo: {'SIMULACIÓN' if self.simulation_mode else 'BORRADO REAL'}")
            
            try:
                total_products = await self.get_total_products(session)
                if total_products == 0:
                    print("No se encontraron productos para borrar.")
                    return

                total_batches = (total_products + self.batch_size - 1) // self.batch_size
                processed_count = 0

                print(f"\nIniciando proceso de borrado:")
                print(f"Total de productos: {total_products}")
                print(f"Tamaño de lote: {self.batch_size}")
                print(f"Operaciones concurrentes: {self.max_concurrent}")
                print(f"Número de lotes: {total_batches}")
                print("\nProgreso:")

                pbar = tqdm(total=total_products, desc="Progreso total", unit="productos")
                status_bar = tqdm(total=0, bar_format='{desc}', position=1)

                try:
                    current_batch = 0
                    while processed_count < total_products:
                        try:
                            products = await self.get_product_batch(session, self.batch_size)
                            if not products:
                                remaining = total_products - processed_count
                                if remaining > 0:
                                    pbar.update(remaining)
                                break

                            current_batch += 1
                            
                            successful, failed = await self.delete_product_batch(session, products)
                            
                            # Procesar resultados
                            self.processed_products.extend(successful)
                            for product, error in failed:
                                self.errors.append(f"Error al borrar {product['title']} (ID: {product['id']}): {error}")
                            
                            # Actualizar progreso
                            processed_count += len(products)
                            self.update_progress(pbar, status_bar, len(products), current_batch, total_batches)
                            
                            # Breve pausa entre lotes
                            await asyncio.sleep(0.5)

                        except aiohttp.ClientResponseError as e:
                            if e.status == 429:
                                tqdm.write("Límite de API alcanzado, esperando 30 segundos...")
                                await asyncio.sleep(30)
                                continue
                            else:
                                raise

                finally:
                    pbar.close()
                    status_bar.close()
                    print("\n¡Proceso completado!")

                # Generar reporte
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                mode = "simulacion" if self.simulation_mode else "real"
                filename = f"reporte_borrado_{mode}_{timestamp}.json"
                
                report = {
                    "fecha_ejecucion": datetime.now().isoformat(),
                    "modo": "Simulación" if self.simulation_mode else "Real",
                    "tienda": self.shop_url,
                    "total_productos_procesados": len(self.processed_products),
                    "productos_procesados": [{"id": p["id"], "title": p["title"]} for p in self.processed_products],
                    "errores": self.errors
                }
                
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
                
                print("\n=== Resumen del proceso ===")
                print(f"Total de productos procesados: {len(self.processed_products)}")
                if self.errors:
                    print(f"\nErrores encontrados: {len(self.errors)}")
                    for error in self.errors[:5]:
                        print(f"- {error}")
                    if len(self.errors) > 5:
                        print(f"... y {len(self.errors) - 5} errores más (ver reporte completo)")
                print(f"\nReporte detallado guardado en: {filename}")

            except Exception as e:
                print(f"Error durante el proceso: {str(e)}")

def main():
    # Configuración
    SHOP_URL = os.getenv('SHOP_URL')
    ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
    
    print("\n=== Sistema de Borrado Masivo de Productos Shopify (Optimizado) ===")
    print(f"Tienda objetivo: {SHOP_URL}")
    print("\nModos disponibles:")
    print("1. Simulación (no borra realmente los productos)")
    print("2. Borrado real")
    
    modo = input("\nSeleccione el modo (1 o 2): ").strip()
    
    if modo not in ['1', '2']:
        print("Opción no válida. Saliendo...")
        return
    
    simulation_mode = (modo == '1')
    
    if simulation_mode:
        print("\nMODO SIMULACIÓN ACTIVADO - No se borrarán productos realmente")
    else:
        print("\n¡ADVERTENCIA! MODO BORRADO REAL")
        print("Esta acción borrará TODOS los productos de la tienda.")
        confirmacion = input("¿Está seguro que desea continuar? (escriba 'SI' para confirmar): ")
        
        if confirmacion != "SI":
            print("Operación cancelada.")
            return
    
    try:
        deleter = ShopifyBulkDeleter(
            shop_url=SHOP_URL,
            access_token=ACCESS_TOKEN,
            simulation_mode=simulation_mode
        )
        asyncio.run(deleter.delete_all_products())
    except Exception as e:
        print(f"\nError: {e}")
        print("\nPor favor verifica:")
        print("1. Que el access token sea válido")
        print("2. Que el token tenga los permisos necesarios (read_products, write_products)")
        print("3. Que la URL de la tienda sea correcta")

if __name__ == "__main__":
    main()