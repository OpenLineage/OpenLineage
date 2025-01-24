import * as React from "react";
import { styled } from "@mui/material/styles";
import Head from "@docusaurus/Head";
import Footer from "../components/footer";
import Layout from "@theme/Layout";
import Card from "@mui/material/Card";
import CardActions from "@mui/material/CardActions";
import CardActionArea from "@mui/material/CardActionArea";
import CardContent from "@mui/material/CardContent";
import CardMedia from "@mui/material/CardMedia";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import Grid from "@mui/material/Unstable_Grid2";
import Collapse from "@mui/material/Collapse";
import IconButton, { IconButtonProps } from "@mui/material/IconButton";
import { Consumers, Partner } from "@site/static/ecosystem/consumers";
import { Producers } from "@site/static/ecosystem/producers";

interface ExpandMoreProps extends IconButtonProps {
  expand: boolean;
}

const ExpandMore = styled((props: ExpandMoreProps) => {
  const { expand, ...other } = props;
  return <IconButton {...other} />;
})(({ theme }) => ({
  marginX: "auto",
  transition: theme.transitions.create("transform", {
    duration: theme.transitions.duration.shortest,
  }),
}));

const LogoCard = (partner: Partner) => {
  const [expanded, setExpanded] = React.useState(false);

  const handleExpandClick = () => {
    setExpanded(!expanded);
  };

  const DisplayLinks = () => {
    if (partner.docs_url) {
      return (
        <CardActions disableSpacing>
          <Button size="small" href={partner.docs_url}>
            Learn More
          </Button>
          <Button size="small" href={partner.org_url} sx={{ marginLeft: 2 }}>
            Website
          </Button>
        </CardActions>
      );
    } else {
      return (
        <CardActions disableSpacing>
          <Button size="small" href={partner.org_url}>
            Website
          </Button>
        </CardActions>
      );
    }
  };

  return (
    <Card raised={true} sx={{ minWidth: 340 }}>
      <CardActions disableSpacing sx={{ padding: 0 }}>
        <ExpandMore
          expand={expanded}
          onClick={handleExpandClick}
          aria-expanded={expanded}
          aria-label="show more"
          sx={{ padding: 0 }}
        >
          <CardActionArea sx={{ padding: 0 }}>
            <CardMedia
              sx={{ minWidth: 340, minHeight: 170, padding: 0 }}
              component="img"
              src={require(`@site/static/img/${partner.image}`).default}
              title={partner.org}
            />
          </CardActionArea>
        </ExpandMore>
      </CardActions>
      <Collapse in={expanded} timeout="auto" unmountOnExit>
        <CardContent sx={{ maxWidth: 340 }}>
          <Typography variant="body1" color="text.secondary">
            {partner.description}
          </Typography>
        </CardContent>
        {DisplayLinks()}
      </Collapse>
    </Card>
  );
};

const FillGrid = (partners: Partner[]) => {
  return (
    <Grid container margin="auto" rowSpacing={8} columnSpacing={4} paddingBottom={4} width="85%">
      {partners.map((partner) => (
        <Grid marginX="auto" justifyItems="top">
          {LogoCard(partner)}
        </Grid>
      ))}
    </Grid>
  );
};

export default function Ecosystem(): JSX.Element {
  const seoTitle = "Ecosystem";
  const seoDescription = "A page about systems that interoperate with the OpenLineage specification for data lineage.";

  return (
    <Layout title={seoTitle} description={seoDescription}>
      <Head>
        <meta property="og:image" content="https://openlineage.io/img/ecosystem-thumb.png" />
        <meta property="twitter:image" content="https://openlineage.io/img/ecosystem-thumb.png" />
      </Head>
      <div className="title pt-4 text-center lg:py-14 lg:px-0">
        <h2 className="text-5xl text-color-1">Ecosystem</h2>
      </div>
      <div className="title pb-8 text-center">
        <h3 className="text-4xl text-color-1">Consumers</h3>
      </div>
      <div className="eco-grid-div">{FillGrid(Consumers.sort((a, b) => a.full_name.localeCompare(b.full_name)))}</div>
      <div className="title pt-12 pb-8 text-center">
        <h3 className="text-4xl text-color-1">Producers</h3>
      </div>
      <div className="eco-grid-div">{FillGrid(Producers.sort((a, b) => a.full_name.localeCompare(b.full_name)))}</div>
      <div className="bg-bg">
        <Footer />
      </div>
    </Layout>
  );
}
