# Fluent-plugin-openlineage, a plugin for [Fluentd](https://www.fluentd.org)

[![Gem Version](https://badge.fury.io/rb/fluent-plugin-openlineage.svg)](https://badge.fury.io/rb/fluent-plugin-openlineage)

fluent-plugin-openlineage is a Fluentd plugin that verifies if a JSON matches the OpenLineage schema. 
It is intended to be used together with a [Fluentd Application](https://github.com/fluent/fluentd).

## Requirements

| fluent-plugin-prometheus | fluentd    | ruby   |
|--------------------------|------------|--------|
| 1.x.y                    | >= v1.9.1  | >= 2.4 |
| 1.[0-7].y                | >= v0.14.8 | >= 2.1 |
| 0.x.y                    | >= v0.12.0 | >= 1.9 |

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'fluent-plugin-openlineage'
```

And then execute:

    $ bundle

Or install it yourself using one of the following:

    $ gem install fluent-plugin-openlineage

    $ fluent-gem install fluent-plugin-openlineage

## Usage

fluentd-plugin-openlineage include only one plugin.

- `openlineage` parse plugin

