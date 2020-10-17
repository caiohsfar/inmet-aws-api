

const client = require('axios').default;
const kinesis = require('./kinesis');
process.env.TZ = 'America/Sao_Paulo'
const datetime = require('node-datetime').create();

datetime.offsetInDays(-1);
const pastDay = datetime.format('Y-m-d');



const getStationsByType = async (type) => {
  const stationsUrl = `https://apitempo.inmet.gov.br/estacoes/${type}`;
  let peStations = [];
  try {
    const { data } = await client.get(stationsUrl);
    peStations = data.filter(item => item.SG_ESTADO == "PE");
  } catch (err) {
    console.log(err.message);
  }

  return peStations;
}


const getAllStations = async () => {
  const manuals = await getStationsByType("T");
  const auto = await getStationsByType("M");

  return [...manuals, ...auto];
}


const getDailyInfoAndSaveIntoKinesis = async () => {
  const stations = await getAllStations();

  const promisses = stations.map(async ({ CD_ESTACAO }) => {
    const dailyUrl = `https://apitempo.inmet.gov.br/estacao/diaria/${pastDay}/${pastDay}/${CD_ESTACAO}`
    try {
      const { data } = await client.get(dailyUrl);
      const { TEMP_MED, TEMP_MIN, TEMP_MAX, UMID_MED, DT_MEDICAO, DC_NOME } = data[0]
      const filteredData = {
        TEMP_MED,
        TEMP_MIN,
        TEMP_MAX,
        UMID_MED,
        DT_MEDICAO,
        DC_NOME
      }
      const response = { [CD_ESTACAO]: filteredData }
      kinesis.save(response);
      return response
    } catch (err) {
      console.log(err.message)
      return {}

    }
  })

  return await Promise.all(promisses);
}



const successfullResponse = {
  isBase64Encoded: false,
  statusCode: 200,
  headers: { 'Content-Type': 'application/json' },
  body: {}
};



exports.handler = async (event, context, callback) => {
  const result = await getDailyInfoAndSaveIntoKinesis();
  successfullResponse.body = { message: "Records added  successfully", data: result };
  callback(null, successfullResponse);
};