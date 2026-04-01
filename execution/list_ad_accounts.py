"""
Script utilitário para listar contas de anúncios de um Business Manager.

Objetivo: Validar descoberta de contas no Business e verificar IDs disponíveis.
"""

import os
import sys

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from execution.meta_client import list_business_ad_accounts

def main():
    """Lista todas as contas de anúncios do Business Manager."""
    try:
        # Carrega variáveis de ambiente
        env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        load_dotenv(env_path)
        
        access_token = os.getenv('META_ACCESS_TOKEN')
        business_id = os.getenv('META_BUSINESS_ID')
        max_retries = int(os.getenv('MAX_RETRIES', '3'))
        
        if not access_token:
            print("ERRO: META_ACCESS_TOKEN não configurada no .env")
            sys.exit(1)
        
        if not business_id:
            print("ERRO: META_BUSINESS_ID não configurada no .env")
            sys.exit(1)
        
        print("=" * 60)
        print("LISTA DE CONTAS DE ANÚNCIOS - BUSINESS MANAGER")
        print("=" * 60)
        print(f"Business ID: {business_id}")
        print(f"Token: {access_token[:20]}...")
        print()
        
        print("Conectando à Meta API...")
        accounts = list_business_ad_accounts(access_token, business_id, max_retries)
        
        if not accounts:
            print("Nenhuma conta de anúncios encontrada no Business.")
            sys.exit(0)
        
        print(f"\nEncontradas {len(accounts)} conta(s) de anúncios:\n")
        print("-" * 60)
        
        for i, account in enumerate(accounts, 1):
            account_id = account.get('id') or account.get('account_id', 'N/A')
            account_name = account.get('name', 'Sem nome')
            account_status = account.get('account_status', 'N/A')
            
            print(f"{i}. Nome: {account_name}")
            print(f"   ID: {account_id}")
            print(f"   Status: {account_status}")
            print()
        
        print("-" * 60)
        print(f"\nTotal: {len(accounts)} conta(s)")
        print("\nUse estes IDs no clients.json para configurar os relatórios.")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\nERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
