"""
Magic command registration for the analyze command.
"""

from data_agency.commands.describe.agent import DataDescribeAgent


def describe(line="", cell=""):
    agent = DataDescribeAgent()
    agent.run(line, cell)
