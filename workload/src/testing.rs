pub(crate) const TINY_CLUSTER: &str = r#"{
  "planes": [
    [
      {
        "id": 16,
        "kind": "Switch"
      }
    ],
    [
      {
        "id": 17,
        "kind": "Switch"
      }
    ]
  ],
  "pods": [
    {
      "fabs": [
        {
          "id": 12,
          "kind": "Switch"
        },
        {
          "id": 13,
          "kind": "Switch"
        }
      ],
      "racks": [
        {
          "tor": {
            "id": 8,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 0,
              "kind": "Host"
            },
            {
              "id": 1,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 0,
              "b": 8,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 1,
              "b": 8,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        },
        {
          "tor": {
            "id": 9,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 2,
              "kind": "Host"
            },
            {
              "id": 3,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 2,
              "b": 9,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 3,
              "b": 9,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        }
      ],
      "tor2fab": [
        {
          "a": 8,
          "b": 12,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 8,
          "b": 13,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 9,
          "b": 12,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 9,
          "b": 13,
          "bandwidth": 40000000000,
          "delay": 1000
        }
      ]
    },
    {
      "fabs": [
        {
          "id": 14,
          "kind": "Switch"
        },
        {
          "id": 15,
          "kind": "Switch"
        }
      ],
      "racks": [
        {
          "tor": {
            "id": 10,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 4,
              "kind": "Host"
            },
            {
              "id": 5,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 4,
              "b": 10,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 5,
              "b": 10,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        },
        {
          "tor": {
            "id": 11,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 6,
              "kind": "Host"
            },
            {
              "id": 7,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 6,
              "b": 11,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 7,
              "b": 11,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        }
      ],
      "tor2fab": [
        {
          "a": 10,
          "b": 14,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 10,
          "b": 15,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 11,
          "b": 14,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 11,
          "b": 15,
          "bandwidth": 40000000000,
          "delay": 1000
        }
      ]
    }
  ],
  "fab2spine": [
    {
      "a": 12,
      "b": 16,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 13,
      "b": 17,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 14,
      "b": 16,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 15,
      "b": 17,
      "bandwidth": 40000000000,
      "delay": 1000
    }
  ]
}"#;

pub(crate) const TINY_CLUSTER_UNORDERED: &str = r#"{
  "fab2spine": [
    {
      "a": 2,
      "b": 0,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 3,
      "b": 1,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 10,
      "b": 0,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 11,
      "b": 1,
      "bandwidth": 40000000000,
      "delay": 1000
    }
  ],
  "planes": [
    [
      {
        "id": 0,
        "kind": "Switch"
      }
    ],
    [
      {
        "id": 1,
        "kind": "Switch"
      }
    ]
  ],
  "pods": [
    {
      "fabs": [
        {
          "id": 2,
          "kind": "Switch"
        },
        {
          "id": 3,
          "kind": "Switch"
        }
      ],
      "racks": [
        {
          "host2tor": [
            {
              "a": 4,
              "b": 6,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 5,
              "b": 6,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 4,
              "kind": "Host"
            },
            {
              "id": 5,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 6,
            "kind": "Switch"
          }
        },
        {
          "host2tor": [
            {
              "a": 7,
              "b": 9,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 8,
              "b": 9,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 7,
              "kind": "Host"
            },
            {
              "id": 8,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 9,
            "kind": "Switch"
          }
        }
      ],
      "tor2fab": [
        {
          "a": 6,
          "b": 2,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 6,
          "b": 3,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 9,
          "b": 2,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 9,
          "b": 3,
          "bandwidth": 40000000000,
          "delay": 1000
        }
      ]
    },
    {
      "fabs": [
        {
          "id": 10,
          "kind": "Switch"
        },
        {
          "id": 11,
          "kind": "Switch"
        }
      ],
      "racks": [
        {
          "host2tor": [
            {
              "a": 12,
              "b": 14,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 13,
              "b": 14,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 12,
              "kind": "Host"
            },
            {
              "id": 13,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 14,
            "kind": "Switch"
          }
        },
        {
          "host2tor": [
            {
              "a": 15,
              "b": 17,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 16,
              "b": 17,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 15,
              "kind": "Host"
            },
            {
              "id": 16,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 17,
            "kind": "Switch"
          }
        }
      ],
      "tor2fab": [
        {
          "a": 14,
          "b": 10,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 14,
          "b": 11,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 17,
          "b": 10,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 17,
          "b": 11,
          "bandwidth": 40000000000,
          "delay": 1000
        }
      ]
    }
  ]
}"#;

pub(crate) const MEDIUM_CLUSTER: &str = r#"{
  "planes": [
    [
      {
        "id": 78,
        "kind": "Switch"
      },
      {
        "id": 79,
        "kind": "Switch"
      }
    ],
    [
      {
        "id": 80,
        "kind": "Switch"
      },
      {
        "id": 81,
        "kind": "Switch"
      }
    ],
    [
      {
        "id": 82,
        "kind": "Switch"
      },
      {
        "id": 83,
        "kind": "Switch"
      }
    ]
  ],
  "pods": [
    {
      "fabs": [
        {
          "id": 72,
          "kind": "Switch"
        },
        {
          "id": 73,
          "kind": "Switch"
        },
        {
          "id": 74,
          "kind": "Switch"
        }
      ],
      "racks": [
        {
          "tor": {
            "id": 64,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 0,
              "kind": "Host"
            },
            {
              "id": 1,
              "kind": "Host"
            },
            {
              "id": 2,
              "kind": "Host"
            },
            {
              "id": 3,
              "kind": "Host"
            },
            {
              "id": 4,
              "kind": "Host"
            },
            {
              "id": 5,
              "kind": "Host"
            },
            {
              "id": 6,
              "kind": "Host"
            },
            {
              "id": 7,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 0,
              "b": 64,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 1,
              "b": 64,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 2,
              "b": 64,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 3,
              "b": 64,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 4,
              "b": 64,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 5,
              "b": 64,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 6,
              "b": 64,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 7,
              "b": 64,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        },
        {
          "tor": {
            "id": 65,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 8,
              "kind": "Host"
            },
            {
              "id": 9,
              "kind": "Host"
            },
            {
              "id": 10,
              "kind": "Host"
            },
            {
              "id": 11,
              "kind": "Host"
            },
            {
              "id": 12,
              "kind": "Host"
            },
            {
              "id": 13,
              "kind": "Host"
            },
            {
              "id": 14,
              "kind": "Host"
            },
            {
              "id": 15,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 8,
              "b": 65,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 9,
              "b": 65,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 10,
              "b": 65,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 11,
              "b": 65,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 12,
              "b": 65,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 13,
              "b": 65,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 14,
              "b": 65,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 15,
              "b": 65,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        },
        {
          "tor": {
            "id": 66,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 16,
              "kind": "Host"
            },
            {
              "id": 17,
              "kind": "Host"
            },
            {
              "id": 18,
              "kind": "Host"
            },
            {
              "id": 19,
              "kind": "Host"
            },
            {
              "id": 20,
              "kind": "Host"
            },
            {
              "id": 21,
              "kind": "Host"
            },
            {
              "id": 22,
              "kind": "Host"
            },
            {
              "id": 23,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 16,
              "b": 66,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 17,
              "b": 66,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 18,
              "b": 66,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 19,
              "b": 66,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 20,
              "b": 66,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 21,
              "b": 66,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 22,
              "b": 66,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 23,
              "b": 66,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        },
        {
          "tor": {
            "id": 67,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 24,
              "kind": "Host"
            },
            {
              "id": 25,
              "kind": "Host"
            },
            {
              "id": 26,
              "kind": "Host"
            },
            {
              "id": 27,
              "kind": "Host"
            },
            {
              "id": 28,
              "kind": "Host"
            },
            {
              "id": 29,
              "kind": "Host"
            },
            {
              "id": 30,
              "kind": "Host"
            },
            {
              "id": 31,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 24,
              "b": 67,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 25,
              "b": 67,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 26,
              "b": 67,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 27,
              "b": 67,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 28,
              "b": 67,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 29,
              "b": 67,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 30,
              "b": 67,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 31,
              "b": 67,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        }
      ],
      "tor2fab": [
        {
          "a": 64,
          "b": 72,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 64,
          "b": 73,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 64,
          "b": 74,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 65,
          "b": 72,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 65,
          "b": 73,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 65,
          "b": 74,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 66,
          "b": 72,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 66,
          "b": 73,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 66,
          "b": 74,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 67,
          "b": 72,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 67,
          "b": 73,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 67,
          "b": 74,
          "bandwidth": 40000000000,
          "delay": 1000
        }
      ]
    },
    {
      "fabs": [
        {
          "id": 75,
          "kind": "Switch"
        },
        {
          "id": 76,
          "kind": "Switch"
        },
        {
          "id": 77,
          "kind": "Switch"
        }
      ],
      "racks": [
        {
          "tor": {
            "id": 68,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 32,
              "kind": "Host"
            },
            {
              "id": 33,
              "kind": "Host"
            },
            {
              "id": 34,
              "kind": "Host"
            },
            {
              "id": 35,
              "kind": "Host"
            },
            {
              "id": 36,
              "kind": "Host"
            },
            {
              "id": 37,
              "kind": "Host"
            },
            {
              "id": 38,
              "kind": "Host"
            },
            {
              "id": 39,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 32,
              "b": 68,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 33,
              "b": 68,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 34,
              "b": 68,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 35,
              "b": 68,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 36,
              "b": 68,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 37,
              "b": 68,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 38,
              "b": 68,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 39,
              "b": 68,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        },
        {
          "tor": {
            "id": 69,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 40,
              "kind": "Host"
            },
            {
              "id": 41,
              "kind": "Host"
            },
            {
              "id": 42,
              "kind": "Host"
            },
            {
              "id": 43,
              "kind": "Host"
            },
            {
              "id": 44,
              "kind": "Host"
            },
            {
              "id": 45,
              "kind": "Host"
            },
            {
              "id": 46,
              "kind": "Host"
            },
            {
              "id": 47,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 40,
              "b": 69,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 41,
              "b": 69,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 42,
              "b": 69,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 43,
              "b": 69,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 44,
              "b": 69,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 45,
              "b": 69,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 46,
              "b": 69,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 47,
              "b": 69,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        },
        {
          "tor": {
            "id": 70,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 48,
              "kind": "Host"
            },
            {
              "id": 49,
              "kind": "Host"
            },
            {
              "id": 50,
              "kind": "Host"
            },
            {
              "id": 51,
              "kind": "Host"
            },
            {
              "id": 52,
              "kind": "Host"
            },
            {
              "id": 53,
              "kind": "Host"
            },
            {
              "id": 54,
              "kind": "Host"
            },
            {
              "id": 55,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 48,
              "b": 70,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 49,
              "b": 70,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 50,
              "b": 70,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 51,
              "b": 70,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 52,
              "b": 70,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 53,
              "b": 70,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 54,
              "b": 70,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 55,
              "b": 70,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        },
        {
          "tor": {
            "id": 71,
            "kind": "Switch"
          },
          "hosts": [
            {
              "id": 56,
              "kind": "Host"
            },
            {
              "id": 57,
              "kind": "Host"
            },
            {
              "id": 58,
              "kind": "Host"
            },
            {
              "id": 59,
              "kind": "Host"
            },
            {
              "id": 60,
              "kind": "Host"
            },
            {
              "id": 61,
              "kind": "Host"
            },
            {
              "id": 62,
              "kind": "Host"
            },
            {
              "id": 63,
              "kind": "Host"
            }
          ],
          "host2tor": [
            {
              "a": 56,
              "b": 71,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 57,
              "b": 71,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 58,
              "b": 71,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 59,
              "b": 71,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 60,
              "b": 71,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 61,
              "b": 71,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 62,
              "b": 71,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 63,
              "b": 71,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ]
        }
      ],
      "tor2fab": [
        {
          "a": 68,
          "b": 75,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 68,
          "b": 76,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 68,
          "b": 77,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 69,
          "b": 75,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 69,
          "b": 76,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 69,
          "b": 77,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 70,
          "b": 75,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 70,
          "b": 76,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 70,
          "b": 77,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 71,
          "b": 75,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 71,
          "b": 76,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 71,
          "b": 77,
          "bandwidth": 40000000000,
          "delay": 1000
        }
      ]
    }
  ],
  "fab2spine": [
    {
      "a": 72,
      "b": 78,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 72,
      "b": 79,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 73,
      "b": 80,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 73,
      "b": 81,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 74,
      "b": 82,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 74,
      "b": 83,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 75,
      "b": 78,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 75,
      "b": 79,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 76,
      "b": 80,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 76,
      "b": 81,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 77,
      "b": 82,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 77,
      "b": 83,
      "bandwidth": 40000000000,
      "delay": 1000
    }
  ]
}"#;
