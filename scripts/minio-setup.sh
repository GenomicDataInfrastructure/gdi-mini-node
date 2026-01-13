#!/usr/bin/env sh

# MinIO configuration script for local development.
# Copies local ./data/ to the "mini-data" bucket, and creates
# access-credentials for the application.
# Currently uses default admin credentials for performing the tasks as its just
# for a development environment.

echo "Connecting:"
mc alias set server http://minis3:9000 minioadmin minioadmin

echo
echo "Adding bucket:"
mc mb server/mini-data

echo
echo "Adding data:"
mc cp -r /data/ server/mini-data/demo/

cat > /tmp/policy-app.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:ListenBucketNotification"
      ],
      "Resource": "arn:aws:s3:::mini-data"
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::mini-data/*"
    }
  ]
}
EOF

echo
echo "Generating access key:"
mc admin accesskey create server/ minioadmin \
   --policy /tmp/policy-app.json \
   --name "gdi-mini-node-app" \
   --description "Used by gdi-mini-node"

echo
echo "All done."
