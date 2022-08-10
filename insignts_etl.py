from facebook_business.objects import AdCampaign
campaign = AdCampaign('23849295425920086')
access_token = 'your_access_token'
params = {
    'date_preset': 'last_year',
}
fields = [
    'account_id',
    'account_name',
    'campaign_id',
    'campaign_name',
    'account_currency',
    'reach',
    'impressions',
    'clicks',
    'cpc',
    'spend'
]
insights = campaign.get_insights(params=params)
print(insights)
