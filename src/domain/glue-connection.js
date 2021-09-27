export default class GlueConnection {
    constructor(name, accountId) {
        this.name = name;
        this.accountId = accountId;
    }

    setName(role) {
        this.role = role
    }

    setType(connectionType) {
        this.connectionType = connectionType
    }

    setDescription(description) {
        this.description = description
    }

    setMatchCriteria(matchCriteria) {
        this.matchCriteria = matchCriteria
    }

    setDBUri(dbUri) {
        this.dbUri = dbUri
    }

    setDBUsername(dbUsername) {
        this.dbUsername = dbUsername
    }

    setDBPassword(dbPassword) {
        this.dbPassword = dbPassword
    }

    setSecurityGroup(securityGroup) {
        this.securityGroup = securityGroup
    }

    setSubnet(subnet) {
        this.subnet = subnet
    }

    getCFGlueConnection() {
        let cfn = {
            Type: "AWS::Glue::Connection",
            Properties: {
                CatalogId: this.accountId,
                ConnectionInput: {
                    ConnectionProperties: {
                        JDBC_CONNECTION_URL: this.dbUri,
                        USER_NAME: this.dbUsername,
                        PASSWORD: this.dbPassword,
                        JDBC_ENFORCE_SSL: false
                    },
                    ConnectionType: this.connectionType,
                    Description: this.description,
                    MatchCriteria: this.matchCriteria,
                    Name: this.name,
                    PhysicalConnectionRequirements: {
                        SecurityGroupIdList: this.securityGroup,
                        SubnetId: this.subnet
                    }
                }
            }
        };

        return cfn;
    }

}
