# UserTesting Functions

- [Pendo.io Destination Function](https://github.com/segment-services-eng/usertesting/blob/main/pendoDestinationFunction.js)
  - Destination Function to enrich Pendo Visitor & Account profiles with traits from Personas
  - Use the Metadata Endpoint
    - To update Pendo Visitor objects, use Segment `userId`
    - To update Pendo Account objects, use Segment `groupId`
  - The current script uses these endpoints:
    - url = self.base_url + "metadata/visitor/agent/value"
    - url = self.base_url + "metadata/account/custom/value"
