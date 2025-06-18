-- Customer Segmentation Analysis using RFM (Recency, Frequency, Monetary) Model
-- This demonstrates advanced SQL skills including window functions, CTEs, and statistical analysis

-- RFM Analysis for Customer Segmentation
WITH customer_rfm_base AS (
    SELECT 
        dc.customer_key,
        dc.customer_id,
        dc.first_name,
        dc.last_name,
        dc.email,
        dc.customer_segment,
        
        -- Recency: Days since last purchase
        EXTRACT(DAY FROM CURRENT_DATE - MAX(dd.full_date)) as recency_days,
        
        -- Frequency: Number of transactions
        COUNT(DISTINCT fs.order_id) as frequency_orders,
        
        -- Monetary: Total amount spent
        SUM(fs.total_amount) as monetary_value,
        
        -- Additional metrics
        AVG(fs.total_amount) as avg_order_value,
        SUM(fs.quantity) as total_items_purchased,
        MIN(dd.full_date) as first_purchase_date,
        MAX(dd.full_date) as last_purchase_date
        
    FROM dim_customer dc
    INNER JOIN fact_sales fs ON dc.customer_key = fs.customer_key
    INNER JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE dc.is_current = TRUE
        AND dd.full_date >= CURRENT_DATE - INTERVAL '2 years'
    GROUP BY dc.customer_key, dc.customer_id, dc.first_name, dc.last_name, 
             dc.email, dc.customer_segment
),

rfm_scores AS (
    SELECT *,
        -- RFM Scoring using NTILE function (5 quintiles)
        NTILE(5) OVER (ORDER BY recency_days DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency_orders ASC) as f_score,
        NTILE(5) OVER (ORDER BY monetary_value ASC) as m_score,
        
        -- Percentile rankings
        PERCENT_RANK() OVER (ORDER BY recency_days) as recency_percentile,
        PERCENT_RANK() OVER (ORDER BY frequency_orders) as frequency_percentile,
        PERCENT_RANK() OVER (ORDER BY monetary_value) as monetary_percentile
        
    FROM customer_rfm_base
),

rfm_segments AS (
    SELECT *,
        CONCAT(r_score, f_score, m_score) as rfm_score,
        
        -- Customer Segment Classification
        CASE 
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 4 AND m_score >= 4 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score >= 2 AND m_score >= 3 THEN 'Potential Loyalists'
            WHEN r_score >= 4 AND f_score <= 2 AND m_score <= 2 THEN 'New Customers'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Promising'
            WHEN r_score >= 2 AND f_score >= 3 AND m_score >= 3 THEN 'Need Attention'
            WHEN r_score >= 2 AND f_score <= 2 AND m_score >= 3 THEN 'About to Sleep'
            WHEN r_score <= 2 AND f_score >= 2 AND m_score >= 2 THEN 'At Risk'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Cannot Lose Them'
            WHEN r_score <= 1 AND f_score >= 2 AND m_score >= 2 THEN 'Hibernating'
            ELSE 'Lost'
        END as customer_segment_rfm,
        
        -- Lifetime Value Calculation
        CASE 
            WHEN recency_days <= 30 THEN monetary_value * 1.2
            WHEN recency_days <= 90 THEN monetary_value * 1.0
            WHEN recency_days <= 180 THEN monetary_value * 0.8
            ELSE monetary_value * 0.5
        END as estimated_clv
        
    FROM rfm_scores
)

-- Final Customer Segmentation Report
SELECT 
    customer_segment_rfm,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage_of_customers,
    
    -- Segment Performance Metrics
    ROUND(AVG(recency_days), 1) as avg_recency_days,
    ROUND(AVG(frequency_orders), 1) as avg_frequency,
    ROUND(AVG(monetary_value), 2) as avg_monetary_value,
    ROUND(AVG(avg_order_value), 2) as avg_order_value,
    ROUND(AVG(estimated_clv), 2) as avg_estimated_clv,
    
    -- Revenue Contribution
    ROUND(SUM(monetary_value), 2) as total_revenue,
    ROUND(SUM(monetary_value) * 100.0 / SUM(SUM(monetary_value)) OVER(), 2) as revenue_percentage,
    
    -- Recommendations
    CASE 
        WHEN customer_segment_rfm = 'Champions' THEN 'Reward, VIP programs, exclusive offers'
        WHEN customer_segment_rfm = 'Loyal Customers' THEN 'Upsell higher value products, loyalty rewards'
        WHEN customer_segment_rfm = 'Potential Loyalists' THEN 'Membership programs, recommend products'
        WHEN customer_segment_rfm = 'New Customers' THEN 'Onboarding, welcome series, support'
        WHEN customer_segment_rfm = 'Promising' THEN 'Create brand awareness, offer trials'
        WHEN customer_segment_rfm = 'Need Attention' THEN 'Limited time offers, recommend based on purchase history'
        WHEN customer_segment_rfm = 'About to Sleep' THEN 'Share valuable resources, recommend popular products'
        WHEN customer_segment_rfm = 'At Risk' THEN 'Send personalized emails, offer discounts'
        WHEN customer_segment_rfm = 'Cannot Lose Them' THEN 'Win back campaigns, ignore otherwise'
        WHEN customer_segment_rfm = 'Hibernating' THEN 'Recreate brand value, offer relevant products'
        ELSE 'Revive interest with new products, ignore otherwise'
    END as marketing_recommendation

FROM rfm_segments
GROUP BY customer_segment_rfm
ORDER BY avg_estimated_clv DESC;

-- Individual Customer Details for Marketing Campaigns
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    customer_segment_rfm,
    rfm_score,
    recency_days,
    frequency_orders,
    monetary_value,
    avg_order_value,
    estimated_clv,
    
    -- Campaign Targeting Flags
    CASE WHEN customer_segment_rfm IN ('Champions', 'Loyal Customers') THEN TRUE ELSE FALSE END as vip_campaign,
    CASE WHEN customer_segment_rfm IN ('At Risk', 'Cannot Lose Them') THEN TRUE ELSE FALSE END as retention_campaign,
    CASE WHEN customer_segment_rfm IN ('New Customers', 'Promising') THEN TRUE ELSE FALSE END as acquisition_campaign,
    CASE WHEN recency_days > 180 THEN TRUE ELSE FALSE END as winback_campaign
    
FROM rfm_segments
WHERE customer_segment_rfm NOT IN ('Lost', 'Hibernating')
ORDER BY estimated_clv DESC; 