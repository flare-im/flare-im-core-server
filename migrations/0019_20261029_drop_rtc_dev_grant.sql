-- Calls moved to the flare.call plugin; Core no longer dispatches rtc.* capabilities, so the
-- development bootstrap grant for them goes. Data only: the table stays.
DELETE FROM capability_user_grants
WHERE tenant_id = '0' AND user_id = '*' AND capability_id = 'rtc.*' AND source = 'init_bootstrap';
