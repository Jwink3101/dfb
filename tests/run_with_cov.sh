pytest --cov dfb --cov-report html \
    test_backup_restore.py \
    test_listing.py \
    test_prune.py \
    test_rcloneapi.py \
    test_timestamp_parser.py
    
# Comment out the rcloneapi test to make sure see where we are using that for
# later deprecation