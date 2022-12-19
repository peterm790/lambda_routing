#!/usr/bin/env python3
import os

import aws_cdk as cdk

from lambda_routing.lambda_routing_stack import LambdaRoutingStack


app = cdk.App()
LambdaRoutingStack(app, "LambdaRoutingStack")

app.synth()
