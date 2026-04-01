"""
Script de teste rápido para validar dados da Meta API.

Busca dados das últimas 24h e imprime resumo das métricas no terminal
para validação com o Gerenciador de Anúncios da Meta.
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.meta_client import get_meta_client
from execution.data_processor import DataProcessor, format_currency, format_number

# Carrega variáveis de ambiente
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path)


def get_period_dates():
    """Calcula as datas das últimas 24 horas fechadas."""
    now = datetime.now()
    
    # Período: últimas 24 horas fechadas
    period_end = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    period_start = (now - timedelta(days=2)).strftime('%Y-%m-%d')
    
    return period_start, period_end


def main():
    """Função principal do teste."""
    print("=" * 60)
    print("TESTE DE BUSCA - META API")
    print("=" * 60)
    print()
    
    try:
        # Calcula período
        period_start, period_end = get_period_dates()
        print(f"Período: {period_start} até {period_end}")
        print()
        
        # Cria clientes
        print("Conectando à Meta API...")
        meta_client = get_meta_client()
        data_processor = DataProcessor()
        
        # Busca insights no nível de conta
        print("Buscando insights no nível de conta...")
        insights = meta_client.get_account_insights(period_start, period_end)
        print(f"Encontrados {len(insights)} registro(s) de insights")
        print()
        
        # Processa métricas
        print("Processando métricas...")
        metrics = data_processor.aggregate_metrics(insights)
        
        # Mostra resumo formatado
        print("=" * 60)
        print("RESUMO DAS MÉTRICAS - ÚLTIMAS 24H")
        print("=" * 60)
        print()
        print(f"Investimento (Spend):        {format_currency(metrics['spend'])}")
        print(f"Impressões:                  {format_number(metrics['impressions'])}")
        print(f"Cliques:                     {format_number(metrics['clicks'])}")
        print(f"CPC (Custo por Clique):      {format_currency(metrics['cpc'])}")
        print(f"CPM (Custo por 1000 imp.):   {format_currency(metrics['cpm'])}")
        print(f"Conversões (Lead + WhatsApp): {format_number(metrics['conversions'])}")
        print(f"CPA (Custo por Conversão):   {format_currency(metrics['cpa'])}")
        print()
        
        # Detalhes das conversões
        print("=" * 60)
        print("DETALHES DAS CONVERSÕES")
        print("=" * 60)
        print()
        
        total_lead = 0
        total_whatsapp = 0
        
        for insight in insights:
            actions = insight.get('actions', [])
            if actions:
                for action in actions:
                    action_type = action.get('action_type', '').lower()
                    value = int(action.get('value', 0))
                    
                    if 'lead' in action_type:
                        total_lead += value
                        print(f"  Lead: {value} (tipo: {action.get('action_type', 'N/A')})")
                    elif 'whatsapp' in action_type or 'whats_app' in action_type:
                        total_whatsapp += value
                        print(f"  WhatsApp: {value} (tipo: {action.get('action_type', 'N/A')})")
        
        print()
        print(f"Total Lead:      {format_number(total_lead)}")
        print(f"Total WhatsApp:  {format_number(total_whatsapp)}")
        print(f"Total Geral:     {format_number(total_lead + total_whatsapp)}")
        print()
        
        # Busca dados de anúncios para detalhes
        print("=" * 60)
        print("BUSCANDO DADOS DETALHADOS DE ANÚNCIOS...")
        print("=" * 60)
        print()
        
        ads = meta_client.get_ads_with_insights(period_start, period_end)
        print(f"Encontrados {len(ads)} anúncio(s) com spend > 0")
        print()
        
        if ads:
            # Criativo campeão
            champion = data_processor.find_champion_creative(ads)
            if champion:
                print("=" * 60)
                print("CRIATIVO CAMPEÃO (Maior volume de conversões)")
                print("=" * 60)
                print()
                print(f"Campanha:        {champion['campaign_name']}")
                print(f"Conjunto (Adset): {champion['adset_name']}")
                print(f"Criativo (Ad):    {champion['ad_name']}")
                print(f"Conversões:       {format_number(champion['conversions'])}")
                print(f"Investimento:     {format_currency(champion['metrics']['spend'])}")
                print(f"CPA:             {format_currency(champion['metrics']['cpa'])}")
                print()
            else:
                print("Nenhum criativo com conversões encontrado.")
                print()
        
        print("=" * 60)
        print("TESTE CONCLUÍDO COM SUCESSO")
        print("=" * 60)
        print()
        print("Compare os valores acima com o Gerenciador de Anúncios da Meta.")
        print(f"Período no Gerenciador: {period_start} até {period_end}")
        print()
        
    except ValueError as e:
        print()
        print("=" * 60)
        print("ERRO DE AUTENTICAÇÃO")
        print("=" * 60)
        print(f"Erro: {str(e)}")
        print()
        print("Verifique:")
        print("  - META_ACCESS_TOKEN no arquivo .env")
        print("  - META_AD_ACCOUNT_ID no arquivo .env")
        print("  - Token válido e com permissões corretas")
        print()
        sys.exit(1)
        
    except Exception as e:
        print()
        print("=" * 60)
        print("ERRO")
        print("=" * 60)
        print(f"Erro: {str(e)}")
        print()
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
