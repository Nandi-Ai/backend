DATABASES = {
            'default': {
                  'ENGINE': 'django.db.backends.postgresql',
                  'NAME': '{{ DB_NAME }}',
                  'USER': '{{ DB_USER }}',
                  'PASSWORD': '{{ DB_PASSWORD }}',
                  'HOST': '{{ DB_INSTANCE }}'
            }
}

{# can be generated with: tr -dc 'a-z0-9!@#$%^&*(-_=+)' < /dev/urandom | head -c50 #}
SECRET_KEY="{{ DJANGO_SECRET }}"

jh_api_user_token = "{{ USER_TOKEN }}" {# md5(random uuid) #}
jh_api_admin_token = "{{ ADMIN_TOKEN }}" {# md5(random uuid) #}

jh_url = "{{ JH_URL }}" {# the JH link. example: https://msc.app.lynx.md/jh/ #}

{#aws_access_key_id="<the access key. can also be derived from os.environ("xxxx")>"#}
{#aws_secret_access_key="<the access key. can also be derived from os.environ("yyyy")>"#}
{#aws_region = "<example: us-east-2>"#}
{#aws_account_number = "<aws_account_number>"#}

COGNITO_AWS_REGION = '{{ AWS_REGION }}'
COGNITO_USER_POOL = '{{ USER_POOL_ID }}'
COGNITO_AUDIENCE = '{{ APP_CLIENT_ID }}'
HTTP_SCHEME = "https"