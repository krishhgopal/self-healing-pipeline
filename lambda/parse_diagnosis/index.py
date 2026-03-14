"""
Parse Bedrock's diagnosis response and apply safety guardrails.
"""
import json
import re


def handler(event, context):
    """Parse Bedrock diagnosis and extract structured remediation plan."""
    diagnosis_text = event.get('diagnosis', {}).get('diagnosis', '')
    quality_results = event.get('qualityResults', {})
    
    # Extract JSON from Bedrock response
    try:
        json_match = re.search(r'\{[\s\S]*\}', diagnosis_text)
        if json_match:
            diagnosis = json.loads(json_match.group())
        else:
            diagnosis = parse_unstructured_response(diagnosis_text)
    except json.JSONDecodeError:
        diagnosis = parse_unstructured_response(diagnosis_text)
    
    # Normalize the diagnosis
    normalized = {
        'root_cause': diagnosis.get('root_cause', 'Unknown'),
        'severity': normalize_severity(diagnosis.get('severity', 'medium')),
        'recommended_action': normalize_action(diagnosis.get('recommended_action', 'ALERT_ONLY')),
        'fix_instructions': diagnosis.get('fix_instructions', {}),
        'confidence_score': float(diagnosis.get('confidence_score', 0.5))
    }
    
    # Apply safety guardrails
    normalized = apply_safety_guardrails(normalized, quality_results)
    
    return normalized


def normalize_severity(severity):
    """Normalize severity to standard values."""
    severity_map = {
        'critical': 'critical', 'high': 'high', 'medium': 'medium', 'low': 'low',
        'crit': 'critical', 'hi': 'high', 'med': 'medium', 'lo': 'low'
    }
    return severity_map.get(severity.lower(), 'medium')


def normalize_action(action):
    """Normalize action to allowed values."""
    action_map = {
        'auto_fix': 'AUTO_FIX', 'autofix': 'AUTO_FIX', 'fix': 'AUTO_FIX',
        'quarantine': 'QUARANTINE', 'isolate': 'QUARANTINE',
        'rollback': 'ROLLBACK', 'revert': 'ROLLBACK',
        'alert': 'ALERT_ONLY', 'alert_only': 'ALERT_ONLY', 'notify': 'ALERT_ONLY'
    }
    return action_map.get(action.lower().replace(' ', '_'), 'ALERT_ONLY')


def apply_safety_guardrails(diagnosis, quality_results):
    """Apply safety guardrails to prevent dangerous auto-remediation."""
    
    # Never auto-fix critical severity issues
    if diagnosis['severity'] == 'critical':
        diagnosis['recommended_action'] = 'ALERT_ONLY'
        diagnosis['guardrail_applied'] = 'Critical issues require human review'
    
    # Require high confidence for auto-fix
    if diagnosis['recommended_action'] == 'AUTO_FIX' and diagnosis['confidence_score'] < 0.8:
        diagnosis['recommended_action'] = 'ALERT_ONLY'
        diagnosis['guardrail_applied'] = 'Confidence too low for auto-fix'
    
    # Check if too many records affected (>10% of dataset)
    failed_rules = quality_results.get('failed_rules', 0)
    total_rules = quality_results.get('total_rules', 1)
    if total_rules > 0 and failed_rules / total_rules > 0.1:
        diagnosis['recommended_action'] = 'QUARANTINE'
        diagnosis['guardrail_applied'] = 'Too many rules failed - quarantining data'
    
    return diagnosis


def parse_unstructured_response(text):
    """Fallback parser for unstructured Bedrock responses."""
    diagnosis = {
        'root_cause': 'Unable to parse',
        'severity': 'medium',
        'recommended_action': 'ALERT_ONLY',
        'fix_instructions': {},
        'confidence_score': 0.3
    }
    
    text_lower = text.lower()
    
    if 'null' in text_lower or 'missing' in text_lower:
        diagnosis['root_cause'] = 'Missing or null values detected'
    elif 'negative' in text_lower:
        diagnosis['root_cause'] = 'Negative values in non-negative field'
    elif 'duplicate' in text_lower:
        diagnosis['root_cause'] = 'Duplicate records detected'
    elif 'schema' in text_lower or 'type' in text_lower:
        diagnosis['root_cause'] = 'Schema or data type mismatch'
    
    return diagnosis
