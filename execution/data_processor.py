"""
Processador de dados da Meta API.

Este módulo implementa funções determinísticas para processar dados da Meta API,
calcular métricas agregadas, identificar criativos campeões e calcular variações
percentuais entre períodos.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DataProcessor:
    """Processador de dados e métricas da Meta Ads."""
    
    def __init__(self):
        """Inicializa o processador de dados."""
        pass
    
    def extract_conversions(self, actions: List[Dict[str, Any]]) -> int:
        """
        Extrai e soma conversões de Lead e WhatsApp.
        
        Args:
            actions: Lista de ações do insight da Meta API
            
        Returns:
            Total de conversões (Lead + WhatsApp)
        """
        total_conversions = 0
        
        if not actions:
            return total_conversions
        
        for action in actions:
            action_type = action.get('action_type', '').lower()
            value = int(action.get('value', 0))
            
            # Soma eventos de Lead e WhatsApp
            if 'lead' in action_type:
                total_conversions += value
            elif 'whatsapp' in action_type or 'whats_app' in action_type:
                total_conversions += value
        
        return total_conversions
    
    def aggregate_metrics(self, insights: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        Agrega métricas de uma lista de insights.
        
        Args:
            insights: Lista de insights da Meta API
            
        Returns:
            Dicionário com métricas agregadas
        """
        aggregated = {
            'spend': 0.0,
            'impressions': 0,
            'clicks': 0,
            'conversions': 0
        }
        
        for insight in insights:
            # Métricas básicas
            aggregated['spend'] += float(insight.get('spend', 0))
            aggregated['impressions'] += int(insight.get('impressions', 0))
            aggregated['clicks'] += int(insight.get('clicks', 0))
            
            # Conversões (Lead + WhatsApp)
            actions = insight.get('actions', [])
            aggregated['conversions'] += self.extract_conversions(actions)
        
        # Calcula métricas derivadas
        if aggregated['clicks'] > 0:
            aggregated['cpc'] = aggregated['spend'] / aggregated['clicks']
        else:
            aggregated['cpc'] = 0.0
        
        if aggregated['impressions'] > 0:
            aggregated['cpm'] = (aggregated['spend'] / aggregated['impressions']) * 1000
        else:
            aggregated['cpm'] = 0.0
        
        if aggregated['conversions'] > 0:
            aggregated['cpa'] = aggregated['spend'] / aggregated['conversions']
        else:
            aggregated['cpa'] = 0.0
        
        return aggregated
    
    def calculate_delta(self, value_a: float, value_b: float) -> str:
        """
        Calcula variação percentual entre dois valores.
        
        Args:
            value_a: Valor do período atual
            value_b: Valor do período comparativo
            
        Returns:
            String com a variação percentual ou "Novo Volume" se value_b for 0
        """
        if value_b == 0:
            return "Novo Volume"
        
        delta = ((value_a - value_b) / value_b) * 100
        
        if delta >= 0:
            return f"+{delta:.2f}%"
        else:
            return f"{delta:.2f}%"
    
    def find_champion_creative(self, ads_data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Identifica o criativo campeão (anúncio com maior volume de conversões).
        
        Args:
            ads_data: Lista de anúncios com insights
            
        Returns:
            Dicionário com informações do criativo campeão ou None
        """
        if not ads_data:
            return None
        
        champion = None
        max_conversions = 0
        
        for ad in ads_data:
            insights = ad.get('insights', {}).get('data', [])
            
            # Agrega conversões de todos os insights do anúncio
            total_conversions = 0
            for insight in insights:
                actions = insight.get('actions', [])
                total_conversions += self.extract_conversions(actions)
            
            if total_conversions > max_conversions:
                max_conversions = total_conversions
                champion = ad
        
        if not champion:
            return None
        
        # Extrai informações hierárquicas
        adset = champion.get('adset', {})
        campaign = adset.get('campaign', {}) if adset else {}
        
        # Agrega métricas do criativo campeão
        insights = champion.get('insights', {}).get('data', [])
        metrics = self.aggregate_metrics(insights)
        
        champion_info = {
            'ad_name': champion.get('name', 'N/A'),
            'adset_name': adset.get('name', 'N/A') if adset else 'N/A',
            'campaign_name': campaign.get('name', 'N/A') if campaign else 'N/A',
            'conversions': max_conversions,
            'metrics': metrics
        }
        
        return champion_info
    
    def process_periods(
        self,
        period_a_insights: List[Dict[str, Any]],
        period_b_insights: List[Dict[str, Any]],
        period_a_ads: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Processa dados de dois períodos e gera relatório comparativo.
        
        Args:
            period_a_insights: Insights do período atual (nível de conta)
            period_b_insights: Insights do período comparativo (nível de conta)
            period_a_ads: Anúncios do período atual (para identificar campeão)
            
        Returns:
            Dicionário com métricas agregadas e comparações
        """
        # Agrega métricas de cada período
        metrics_a = self.aggregate_metrics(period_a_insights)
        metrics_b = self.aggregate_metrics(period_b_insights)
        
        # Calcula deltas
        deltas = {
            'spend': self.calculate_delta(metrics_a['spend'], metrics_b['spend']),
            'impressions': self.calculate_delta(metrics_a['impressions'], metrics_b['impressions']),
            'clicks': self.calculate_delta(metrics_a['clicks'], metrics_b['clicks']),
            'cpc': self.calculate_delta(metrics_a['cpc'], metrics_b['cpc']),
            'cpm': self.calculate_delta(metrics_a['cpm'], metrics_b['cpm']),
            'conversions': self.calculate_delta(metrics_a['conversions'], metrics_b['conversions']),
            'cpa': self.calculate_delta(metrics_a['cpa'], metrics_b['cpa'])
        }
        
        # Identifica criativo campeão
        champion = self.find_champion_creative(period_a_ads)
        
        return {
            'period_a': metrics_a,
            'period_b': metrics_b,
            'deltas': deltas,
            'champion': champion
        }


def format_currency(value: float) -> str:
    """
    Formata valor monetário em Real (BRL).
    
    Args:
        value: Valor numérico
        
    Returns:
        String formatada como moeda
    """
    return f"R$ {value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def format_number(value: int) -> str:
    """
    Formata número inteiro com separador de milhar.
    
    Args:
        value: Valor inteiro
        
    Returns:
        String formatada
    """
    return f"{value:,}".replace(',', '.')
