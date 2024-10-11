// install (please try to align the version of installed @nivo packages)
// yarn add @nivo/pie
import { ResponsivePie } from "@nivo/pie";

// make sure parent container have a defined height when using
// responsive component, otherwise height will be 0 and
// no chart will be rendered.
// website examples showcase many properties,
// you'll often use just a few of them.

const data = [
  {
    id: "scala",
    label: "scala",
    value: 155,
    color: "hsl(332, 70%, 50%)",
  },
  {
    id: "stylus",
    label: "stylus",
    value: 164,
    color: "hsl(344, 70%, 50%)",
  },
  {
    id: "php",
    label: "php",
    value: 439,
    color: "hsl(55, 70%, 50%)",
  },
  {
    id: "ruby",
    label: "ruby",
    value: 8,
    color: "hsl(355, 70%, 50%)",
  },
  {
    id: "rust",
    label: "rust",
    value: 69,
    color: "hsl(20, 70%, 50%)",
  },
  {
    id: "go",
    label: "go",
    value: 166,
    color: "hsl(197, 70%, 50%)",
  },
  {
    id: "c",
    label: "c",
    value: 31,
    color: "hsl(13, 70%, 50%)",
  },
  {
    id: "make",
    label: "make",
    value: 110,
    color: "hsl(25, 70%, 50%)",
  },
  {
    id: "sass",
    label: "sass",
    value: 12,
    color: "hsl(292, 70%, 50%)",
  },
  {
    id: "elixir",
    label: "elixir",
    value: 266,
    color: "hsl(287, 70%, 50%)",
  },
  {
    id: "javascript",
    label: "javascript",
    value: 365,
    color: "hsl(307, 70%, 50%)",
  },
  {
    id: "java",
    label: "java",
    value: 63,
    color: "hsl(1, 70%, 50%)",
  },
  {
    id: "css",
    label: "css",
    value: 578,
    color: "hsl(8, 70%, 50%)",
  },
  {
    id: "lisp",
    label: "lisp",
    value: 145,
    color: "hsl(346, 70%, 50%)",
  },
  {
    id: "hack",
    label: "hack",
    value: 294,
    color: "hsl(123, 70%, 50%)",
  },
  {
    id: "python",
    label: "python",
    value: 580,
    color: "hsl(338, 70%, 50%)",
  },
  {
    id: "erlang",
    label: "erlang",
    value: 517,
    color: "hsl(242, 70%, 50%)",
  },
  {
    id: "haskell",
    label: "haskell",
    value: 8,
    color: "hsl(155, 70%, 50%)",
  },
];

const MyResponsivePie = () => (
  <ResponsivePie
    data={data}
    margin={{ top: 40, right: 80, bottom: 80, left: 80 }}
    innerRadius={0.5}
    padAngle={0.7}
    cornerRadius={3}
    activeOuterRadiusOffset={8}
    borderWidth={1}
    borderColor={{
      from: "color",
      modifiers: [["darker", 0.2]],
    }}
    arcLinkLabelsSkipAngle={10}
    arcLinkLabelsTextColor="#333333"
    arcLinkLabelsThickness={2}
    arcLinkLabelsColor={{ from: "color" }}
    arcLabelsSkipAngle={10}
    arcLabelsTextColor={{
      from: "color",
      modifiers: [["darker", 2]],
    }}
    defs={[
      {
        id: "dots",
        type: "patternDots",
        background: "inherit",
        color: "rgba(255, 255, 255, 0.3)",
        size: 4,
        padding: 1,
        stagger: true,
      },
      {
        id: "lines",
        type: "patternLines",
        background: "inherit",
        color: "rgba(255, 255, 255, 0.3)",
        rotation: -45,
        lineWidth: 6,
        spacing: 10,
      },
    ]}
    fill={[
      {
        match: {
          id: "ruby",
        },
        id: "dots",
      },
      {
        match: {
          id: "c",
        },
        id: "dots",
      },
      {
        match: {
          id: "go",
        },
        id: "dots",
      },
      {
        match: {
          id: "python",
        },
        id: "dots",
      },
      {
        match: {
          id: "scala",
        },
        id: "lines",
      },
      {
        match: {
          id: "lisp",
        },
        id: "lines",
      },
      {
        match: {
          id: "elixir",
        },
        id: "lines",
      },
      {
        match: {
          id: "javascript",
        },
        id: "lines",
      },
    ]}
    legends={[
      {
        anchor: "bottom",
        direction: "row",
        justify: false,
        translateX: 0,
        translateY: 56,
        itemsSpacing: 0,
        itemWidth: 100,
        itemHeight: 18,
        itemTextColor: "#999",
        itemDirection: "left-to-right",
        itemOpacity: 1,
        symbolSize: 18,
        symbolShape: "circle",
        effects: [
          {
            on: "hover",
            style: {
              itemTextColor: "#000",
            },
          },
        ],
      },
    ]}
  />
);

const GraphSVG = () => {
  return <MyResponsivePie />;
};

export default GraphSVG;
